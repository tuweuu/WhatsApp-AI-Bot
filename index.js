const qrcode = require('qrcode-terminal');
const { Client, LocalAuth, MessageMedia } = require('whatsapp-web.js');
const { OpenAI } = require("openai");
const fs = require('fs').promises;
const fsSync = require('fs');
const pdf = require('pdf-parse');
const ExcelParser = require('./excel-parser');
require('dotenv').config();

// --- CONFIGURATION ---
const OPENAI_MODEL = "gpt-4.1";
const MAX_HISTORY_LENGTH = 20;
const SUMMARIZATION_PROMPT = "Briefly summarize your conversation with the resident. Note down key details, names, and specific requests to ensure a smooth follow-up.";
const HISTORY_FILE_PATH = './history.json';

// --- WORK GROUP INTEGRATION ---
const WORK_GROUP_ID = process.env.WORK_GROUP_ID || null;
const REQUEST_CONFIRMATION_PROMPT = "Read the following message. Does it confirm that a service request has been successfully created and all necessary information (like address and time) has been collected? Answer only with 'yes' or 'no'.";
const REQUEST_EXTRACTION_PROMPT = "Extract the user's address and a description of the issue from the conversation. Return the data in JSON format with the keys: 'address', and 'issue'. If any information is missing, use the value 'null'.";
const ISSUE_SUMMARY_PROMPT = "Summarize the following issue in a two-three words  **in Russian**.";
const DETAILED_ISSUE_PROMPT = "Based on the conversation history, generate a concise description of the user's issue **in Russian**, under 50 words.";
const ACCOUNT_EXTRACTION_PROMPT = "Analyze the ENTIRE conversation history and extract the full name and complete address for the person whose account is being requested. This could be the user themselves or someone they're asking about (like a family member). Information may be provided across multiple messages. Look for: 1) Full name (first name, last name) - may be provided in parts across different messages 2) Complete address including street name, house number, and apartment number - may also be provided in parts. Combine all address parts into a single address string. Return the data in JSON format with the keys: 'fullName' and 'address'. If any information is missing, use the value 'null'. Examples: fullName: 'Адакова Валерия Аликовна', address: 'Магомеда Гаджиева 73а, кв. 92'. Pay special attention to: - Names that may be provided as 'адакова валерия' first, then 'Адакова Валерия Аликовна' later - Addresses like 'магомед гаджиева 73а, 92кв' or 'магомед гаджиева 73а' + '92кв' separately";

const SYSTEM_PROMPT = `Ты - Кристина, администратор УК "Прогресс".

Твои задачи:
- Консультировать по услугам, графику, контактам.
- Принимать заявки на ремонт: перед приемом заявки узнай как можно больше информации, например: проблема во всем доме или в одной квартире? уточни адрес и время, подтверди прием заявки.
- Помогать с оплатой.
- Предоставлять номера лицевых счетов жильцам для входа в приложение (нужны ФИО и точный адрес).
- Фиксировать жалобы.

КОГДА ЖИЛЕЦ ПРОСИТ ЛИЦЕВОЙ СЧЕТ:
- Естественно попроси полное ФИО (фамилия, имя, отчество)
- Попроси точный адрес (улица, дом, квартира)
- Собирай информацию постепенно в ходе беседы
- Когда у тебя есть полное ФИО и адрес, добавь в свой ответ слово LOOKUP_ACCOUNT чтобы система могла найти счет
- Если предоставленная информация не подходит, вежливо попроси уточнить данные
- При неудаче предложи обратиться в офис

Справочная информация:
- График: Пн-Пт, 9:00-18:00.
- Адрес: Ирчи Казака 31.
- Офис: +78004445205.
- Оплата: Переводом на номер: +7 (900) 050 11 11, в офисе или через приложение УК «Прогресс».
  - iOS: https://apps.apple.com/app/id6738488843
  - Android: https://play.google.com/store/apps/details?id=ru.burmistr.app.client.c_4296

Важно:
- Будь профессиональной и четкой. Избегай излишней эмпатии и фраз вроде \"Мы понимаем ваше расстройство\", но продолжай быть вежливой и на фразы "Спасибо" или "До свидания" - отвечай так же тепло.
- Отвечай кратко и по делу. Не предлагай свою помощь каждый раз. Если жильцу нужна помощь - он сам обратится.
- Ссылки отправляй как обычный текст, без форматирования.
- Говори только на русском.
- Не придумывай, если не знаешь ответ.
- Не упоминай, что ты ИИ.

Цель: быстро помочь и оставить приятное впечатление.`;

// --- INITIALIZATION ---
const openai = new OpenAI({
    apiKey: process.env.OPENAI_API_KEY,
});

const client = new Client({
    authStrategy: new LocalAuth()
});

let conversationHistories = {};
let excelParser = null;

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
        await fs.access(HISTORY_FILE_PATH);
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

// --- PDF HANDLING FUNCTION ---
async function handlePdf(media) {
    try {
        console.log("Received PDF, parsing text...");
        const fileBuffer = Buffer.from(media.data, 'base64');
        const data = await pdf(fileBuffer);
        return `The user has sent a PDF. Here is the content: ${data.text}`;
    } catch (error) {
        console.error("Error parsing PDF:", error);
        return "I had trouble reading that PDF file. Please try sending it again.";
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

    const history = conversationHistories[message.from] || [];
    let messageBody = message.body;
    let userHistoryEntry;

    if (message.hasMedia) {
        const media = await message.downloadMedia();
        if (media.mimetype === 'image/jpeg' || media.mimetype === 'image/png' || media.mimetype === 'image/webp') {
             try {
                console.log("Received image message, processing with OpenAI Vision...");
                const openAIContent = [
                    { type: 'text', text: message.body },
                    { type: 'image_url', image_url: { url: `data:${media.mimetype};base64,${media.data}` } }
                ];
                userHistoryEntry = { role: "user", type: 'image', content: openAIContent, media: { mimetype: media.mimetype, data: media.data } };
            } catch (error) {
                console.error("Error processing image:", error);
                message.reply("I had trouble seeing that image. Please try sending it again.");
                return;
            }
        } else if (media.mimetype === 'application/pdf') {
            try {
                console.log("Received PDF message, processing...");
                messageBody = await handlePdf(media);
                userHistoryEntry = { role: "user", type: 'file', content: messageBody, media: { mimetype: media.mimetype, data: media.data, filename: media.filename } };
            } catch (error) {
                console.error("Error processing PDF:", error);
                message.reply("I had trouble reading that PDF. Please try sending it again.");
                return;
            }
        } else if (media.mimetype === 'audio/ogg' || message.type === 'ptt' || message.type === 'audio') {
            try {
                console.log("Received voice message, transcribing...");
                const audioBuffer = Buffer.from(media.data, 'base64');
                const tempFilePath = `./temp_audio_${Date.now()}.ogg`;
                await fs.writeFile(tempFilePath, audioBuffer);

                const transcription = await openai.audio.transcriptions.create({
                    file: fsSync.createReadStream(tempFilePath),
                    model: "whisper-1",
                });

                await fs.unlink(tempFilePath);
                messageBody = transcription.text;
                console.log(`Transcription result: \"${messageBody}\"`);
                userHistoryEntry = { role: "user", type: 'audio', content: messageBody, media: { mimetype: media.mimetype, data: media.data } };
            } catch (error) {
                console.error("Error transcribing audio:", error);
                message.reply("I couldn't understand the audio message. Please try again.");
                return;
            }
        } else {
            // Unsupported file type
            console.log(`Received unsupported file type: ${media.mimetype}`);
            message.reply("I can only analyze images, PDFs, and voice messages at the moment.");
            return;
        }
    } else {
        userHistoryEntry = { role: "user", type: 'text', content: messageBody };
    }


    if (messageBody.toLowerCase() === '!reset') {
        delete conversationHistories[message.from];
        await saveHistory();
        console.log(`History for ${message.from} has been reset.`);
        message.reply("I've cleared our previous conversation. Let's start fresh.");
        return;
    }

    try {
        history.push(userHistoryEntry);

        const aiResponse = await getOpenAIResponse(history);
        
        // Check if AI response indicates it wants to look up an account
        if (aiResponse.includes('LOOKUP_ACCOUNT')) {
            // Try to handle account lookup
            const accountHandled = await handleAccountLookup(message.from, history);
            if (accountHandled) {
                conversationHistories[message.from] = history;
                await saveHistory();
                return;
            }
            // Remove the LOOKUP_ACCOUNT keyword from the response
            const cleanedResponse = aiResponse.replace('LOOKUP_ACCOUNT', '').trim();
            history.push({ role: "assistant", type: 'text', content: cleanedResponse });
            conversationHistories[message.from] = history;
            message.reply(cleanedResponse);
            await saveHistory();
        } else {
            history.push({ role: "assistant", type: 'text', content: aiResponse });
            conversationHistories[message.from] = history;
            message.reply(aiResponse);
            await saveHistory();
        }

        if (await isRequestCreationConfirmation(aiResponse)) {
            await handleServiceRequest(message.from, history);
        }

        if (history.length > MAX_HISTORY_LENGTH) {
            console.log(`History for ${message.from} exceeds limit. Triggering summarization.`);
            await summarizeHistory(message.from);
            await saveHistory();
        }

    } catch (error) {
        console.error("Error processing message:", error);
        message.reply("I'm having some trouble right now. Please try again.");
    }
});

// --- STARTUP SEQUENCE ---
async function start() {
    await loadHistory();
    
    // Initialize Excel parser
    console.log('Initializing Excel parser...');
    excelParser = new ExcelParser();
    await excelParser.loadAllExcelFiles('./');
    console.log('Excel parser initialized.');
    
    client.initialize();
}

start();

// --- CORE AI FUNCTIONS ---

async function getOpenAIResponse(richHistory) {
    try {
        const openAIMessages = richHistory.map(msg => ({
            role: msg.role,
            content: msg.content
        }));

        const completion = await openai.chat.completions.create({
            model: OPENAI_MODEL,
            messages: [{ role: "system", content: SYSTEM_PROMPT }, ...openAIMessages],
            max_tokens: 300
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
        { role: "user", type: 'text', content: SUMMARIZATION_PROMPT }
    ];

    try {
        const summaryResponse = await getOpenAIResponse(summarizationMessages);
        const recentHistory = history.slice(-5);
        conversationHistories[chatId] = [
            { role: "system", type: 'text', content: `Summary of previous conversation: ${summaryResponse}` },
            ...recentHistory
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
            messages: [...history.map(m => ({role: m.role, content: m.content})), { role: "user", content: REQUEST_EXTRACTION_PROMPT }],
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
                messages: [...history.map(m => ({role: m.role, content: m.content})), { role: "user", content: DETAILED_ISSUE_PROMPT }],
                max_tokens: 150
            });
            const detailedIssue = detailedIssueCompletion.choices[0].message.content.trim();

            const requestMessage = `🆕 Новая заявка от жильца\n\n📞 Телефон: ${phone}\n📍 Адрес: ${address}\n❗️ Проблема: ${issueSummary}\n\n📝 Описание:\n${detailedIssue}`;
            await client.sendMessage(WORK_GROUP_ID, requestMessage);
            console.log(`Service request text from ${chatId} sent to work group.`);

            for (const msg of history) {
                if (msg.role === 'user' && msg.media && !msg.forwarded) {
                    try {
                        const media = new MessageMedia(msg.media.mimetype, msg.media.data);
                        let caption = 'Attached media file from user.';
                        if (msg.type === 'image') {
                        } else if (msg.type === 'audio') {
                            caption = `User-submitted voice message. Transcription: \"${msg.content}\"`;
                        }
                        await client.sendMessage(WORK_GROUP_ID, media, { caption });
                        console.log(`Forwarded ${msg.type} from ${chatId} to work group.`);
                        msg.forwarded = true;
                    } catch (e) {
                        console.error(`Failed to forward media from ${chatId} to work group:`, e);
                    }
                }
            }

        } else {
            console.log(`Incomplete information for service request from ${chatId}. The bot will ask for more details.`);
        }
    } catch (error) {
        console.error(`Error handling service request for ${chatId}:`, error);
    }
}

// --- ACCOUNT LOOKUP FUNCTIONS ---



async function handleAccountLookup(chatId, history) {
    if (!excelParser) {
        console.error("Excel parser is not initialized. Cannot perform account lookup.");
        return false;
    }

    try {
        const extractionCompletion = await openai.chat.completions.create({
            model: OPENAI_MODEL,
            messages: [
                { role: "system", content: ACCOUNT_EXTRACTION_PROMPT },
                ...history.map(m => ({role: m.role, content: m.content}))
            ],
            response_format: { type: "json_object" },
        });
        const extractedData = JSON.parse(extractionCompletion.choices[0].message.content);

        const { fullName, address } = extractedData;
        const phone = `+${chatId.split('@')[0]}`;

        if (fullName && address) {
            console.log(`Looking up account for: ${fullName} at ${address}`);
            const accountInfo = excelParser.findResidentAccount(fullName, address);
            
            if (accountInfo) {
                const accountMessage = `🏠 Найден ваш лицевой счет:\n\n📋 Номер: ${accountInfo.accountNumber}\n👤 ФИО: ${accountInfo.fullName}\n🏠 Квартира: ${accountInfo.apartmentNumber}\n📍 Адрес: ${accountInfo.address}\n\nИспользуйте этот номер для входа в приложение УК "Прогресс".`;
                await client.sendMessage(chatId, accountMessage);
                // Add the account info to conversation history
                history.push({ role: "assistant", type: 'text', content: accountMessage });
                console.log(`Account info sent to ${chatId}: ${accountInfo.accountNumber}`);
                return true;
            } else {
                // Account not found - let AI handle this to continue the conversation
                console.log(`Account not found for ${chatId}: ${fullName} at ${address} - letting AI handle response`);
                return false; // Let AI respond and potentially ask for corrections
            }
        } else {
            console.log(`Incomplete information for account lookup from ${chatId}. Missing: ${!fullName ? 'fullName' : ''} ${!address ? 'address' : ''} - falling back to AI`);
            return false; // Let AI handle asking for missing information
        }
    } catch (error) {
        console.error(`Error handling account lookup for ${chatId}:`, error);
        return false;
    }
}