const qrcode = require('qrcode-terminal');
const { Client, LocalAuth } = require('whatsapp-web.js');
const { OpenAI } = require("openai");
const fs = require('fs').promises; // Using promises for async file operations
require('dotenv').config();

// --- CONFIGURATION ---
const OPENAI_MODEL = "gpt-4.1"; // Updated model
const MAX_HISTORY_LENGTH = 20;
const SUMMARIZATION_PROMPT = "Briefly summarize your conversation with the resident. Note down key details, names, and specific requests to ensure a smooth follow-up.";
const HISTORY_FILE_PATH = './history.json';

// --- WORK GROUP INTEGRATION ---
const WORK_GROUP_ID = process.env.WORK_GROUP_ID || null;
const REQUEST_CONFIRMATION_PROMPT = "Read the following message. Does it confirm that a service request has been successfully created and all necessary information (like address and time) has been collected? Answer only with 'yes' or 'no'.";
const REQUEST_EXTRACTION_PROMPT = "Extract the user's address and a description of the issue from the conversation. Return the data in JSON format with the keys: 'address', and 'issue'. If any information is missing, use the value 'null'.";
const ISSUE_SUMMARY_PROMPT = "Summarize the following issue in a few words.";
const DETAILED_ISSUE_PROMPT = "Based on the conversation history, generate a concise description of the user's issue **in Russian**, under 50 words.";

const SYSTEM_PROMPT = `Ты - Кристина. Твоя роль - администратор управляющей компании "Прогресс". Ты общаешься с жильцами и помогаешь им решать бытовые вопросы.

Твои задачи:
- Консультировать по услугам компании, графику работы и контактам.
- Принимать заявки на ремонт. Если жилец хочет оставить заявку, обязательно уточни, что тебе для этого нужны его адрес и удобное время для визита мастера. Получив всю информацию, подтверди, что заявка принята.
- Помогать с вопросами по квитанциям и оплате.
- Фиксировать жалобы и обращения.

Справочная информация:
- График работы: с 9:00 до 18:00, с понедельника по пятницу.
- Телефон офиса: +7 800 444 52 05.
- Оплату можно произвести в мобильном приложении или в офисе управляющей компании.
- Приложение для iOS: https://apps.apple.com/app/id6738488843
- Приложение для Android: https://play.google.com/store/apps/details?id=ru.burmistr.app.client.c_4296

Важно:
- Будь вежливой, профессиональной и отзывчивой.
- Говори только на русском языке.
- Если не знаешь ответа, не придумывай. Сообщи, что уточнишь информацию у коллег.
- Веди себя как живой профессиональный администратор. Не упоминай об ИИ.

Твоя цель — оперативно помочь каждому жильцу и оставить приятное впечатление о работе компании.`;


// --- INITIALIZATION ---
const openai = new OpenAI({
    apiKey: process.env.OPENAI_API_KEY,
});

const client = new Client({
    authStrategy: new LocalAuth()
});

let conversationHistories = {}; // Let it be mutable

// --- PERSISTENCE FUNCTIONS ---
async function saveHistory() {
    try {
        const data = JSON.stringify(conversationHistories, null, 2);
        await fs.writeFile(HISTORY_FILE_PATH, data, 'utf8');
    } catch (error) {
        console.error("Error saving conversation history:", error);
    }
}

async function loadHistory() {
    try {
        await fs.access(HISTORY_FILE_PATH); // Check if file exists
        const data = await fs.readFile(HISTORY_FILE_PATH, 'utf8');
        conversationHistories = JSON.parse(data);
        console.log("Successfully loaded conversation history from file.");
    } catch (error) {
        if (error.code === 'ENOENT') {
            console.log("No history file found. Starting with a fresh history.");
        } else {
            console.error("Error loading conversation history:", error);
        }
    }
}

// --- WHATSAPP CLIENT EVENTS ---
client.on('qr', qr => {
    qrcode.generate(qr, { small: true });
});

client.on('ready', () => {
    console.log('Client is ready!');
});

client.on('message', async message => {
    if (message.from.endsWith('@g.us')) {
        console.log(`Message received from group: ${message.from}`);
        return;
    }    

    if (message.isStatus) return;

    if (message.body.toLowerCase() === '!reset') {
        delete conversationHistories[message.from];
        await saveHistory(); // Save after reset
        console.log(`History for ${message.from} has been reset.`);
        message.reply("I've cleared our previous conversation. Let's start fresh.");
        return;
    }

    try {
        const history = conversationHistories[message.from] || [];
        history.push({ role: "user", content: message.body });

        const aiResponse = await getOpenAIResponse(history);
        history.push({ role: "assistant", content: aiResponse });

        conversationHistories[message.from] = history;
        message.reply(aiResponse);
        await saveHistory(); // Save after each message

        if (await isRequestCreationConfirmation(aiResponse)) {
            await handleServiceRequest(message.from, history);
        }

        if (history.length > MAX_HISTORY_LENGTH) {
            console.log(`History for ${message.from} exceeds limit. Triggering summarization.`);
            await summarizeHistory(message.from);
            await saveHistory(); // Save again after summarization
        }

    } catch (error) {
        console.error("Error processing message:", error);
        message.reply("I'm having some trouble right now. Please try again.");
    }
});

// --- STARTUP SEQUENCE ---
async function start() {
    await loadHistory();
    client.initialize();
}

start();

// --- CORE AI FUNCTIONS ---

async function getOpenAIResponse(messages) {
    try {
        const completion = await openai.chat.completions.create({
            model: OPENAI_MODEL,
            messages: [{ role: "system", content: SYSTEM_PROMPT }, ...messages],
            max_tokens: 300 // Slightly increased for the more capable model
        });
        const response = completion.choices[0].message.content.trim();
        if (!response) throw new Error("Received empty response from OpenAI.");
        return response;
    } catch (error) {
        console.error("Error with OpenAI API call:", error.response ? error.response.data : error.message);
        throw new Error("Failed to get response from OpenAI.");
    }
}

async function summarizeHistory(chatId) {
    const history = conversationHistories[chatId];
    if (!history || history.length === 0) return;

    console.log(`Summarizing ${history.length} messages for chat ${chatId}...`);
    const summarizationMessages = [
        ...history,
        { role: "user", content: SUMMARIZATION_PROMPT }
    ];

    try {
        const summaryResponse = await getOpenAIResponse(summarizationMessages);
        conversationHistories[chatId] = [
            { role: "system", content: `Summary of previous conversation: ${summaryResponse}` }
        ];
        console.log(`Summarization complete for ${chatId}.`);
    } catch (error) {
        console.error(`Failed to summarize history for ${chatId}:`, error);
    }
}

// --- SERVICE REQUEST FUNCTIONS ---

async function isRequestCreationConfirmation(messageContent) {
    try {
        const completion = await openai.chat.completions.create({
            model: OPENAI_MODEL,
            messages: [{
                role: "user",
                content: `${REQUEST_CONFIRMATION_PROMPT}\n\n${messageContent}`
            }],
            max_tokens: 10
        });
        const response = completion.choices[0].message.content.trim().toLowerCase();
        return response.includes('yes');
    } catch (error) {
        console.error("Error identifying request creation confirmation:", error);
        return false;
    }
}

async function handleServiceRequest(chatId, history) {
    if (!WORK_GROUP_ID) {
        console.error("WORK_GROUP_ID is not set in the .env file. Cannot send service request.");
        return;
    }

    try {
        const extractionCompletion = await openai.chat.completions.create({
            model: OPENAI_MODEL,
            messages: [...history, { role: "user", content: REQUEST_EXTRACTION_PROMPT }],
            response_format: { type: "json_object" },
        });
        const extractedData = JSON.parse(extractionCompletion.choices[0].message.content);

        const { address, issue } = extractedData;
        const phone = `+${chatId.split('@')[0]}`;

        if (address && issue) {
            const summaryCompletion = await openai.chat.completions.create({
                model: OPENAI_MODEL,
                messages: [{ role: "user", content: `${ISSUE_SUMMARY_PROMPT}: ${issue}` }],
                max_tokens: 10
            });
            const issueSummary = summaryCompletion.choices[0].message.content.trim();

            const detailedIssueCompletion = await openai.chat.completions.create({
                model: OPENAI_MODEL,
                messages: [...history, { role: "user", content: DETAILED_ISSUE_PROMPT }],
                max_tokens: 150
            });
            const detailedIssue = detailedIssueCompletion.choices[0].message.content.trim();

            const requestMessage = `🆕 Новая заявка от жильца\n\n📞 Телефон: ${phone}\n📍 Адрес: ${address}\n❗️ Проблема: ${issueSummary}\n\n📝 Описание:\n${detailedIssue}`;
            await client.sendMessage(WORK_GROUP_ID, requestMessage);
            console.log(`Service request from ${chatId} sent to work group.`);
        } else {
            console.log(`Incomplete information for service request from ${chatId}. The bot will ask for more details.`);
        }
    } catch (error) {
        console.error(`Error handling service request for ${chatId}:`, error);
    }
}

