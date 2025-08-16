const qrcode = require('qrcode-terminal');
const { Client, LocalAuth, MessageMedia } = require('whatsapp-web.js');
const { OpenAI } = require("openai");
const fs = require('fs').promises;
const fsSync = require('fs');
const pdf = require('pdf-parse');
const ExcelParser = require('./excel-parser');
const { Debouncer } = require('@tanstack/pacer');
const ffmpeg = require('fluent-ffmpeg');
const path = require('path');
require('dotenv').config();

// --- CONFIGURATION ---
const OPENAI_MODEL = "gpt-4.1";
const MAX_HISTORY_LENGTH = 20;
const SUMMARIZATION_PROMPT = "Briefly summarize your conversation with the resident. Note down key details, names, and specific requests to ensure a smooth follow-up.";
const HISTORY_FILE_PATH = './history.json';
const ACCOUNT_EXTRACTION_PROMPT = "Analyze the ENTIRE conversation history and extract the full name and complete address for the person whose account is being requested. This could be the user themselves or someone they're asking about (like a family member). Information may be provided across multiple messages. Look for: 1) Full name (first name, last name) - may be provided in parts across different messages 2) Complete address including street name, house number, and apartment number - may also be provided in parts. Combine all address parts into a single address string. Return the data in JSON format with the keys: 'fullName' and 'address'. If any information is missing, use the value 'null'. Examples: fullName: 'Адакова Валерия Аликовна', address: 'Магомеда Гаджиева 73а, кв. 92'. Pay special attention to: - Names that may be provided as 'адакова валерия' first, then 'Адакова Валерия Аликовна' later - Addresses like 'магомед гаджиева 73а, 92кв' or 'магомед гаджиева 73а' + '92кв' separately";

// --- ADMIN INTEGRATION ---
const ADMIN_GROUP_ID = process.env.ADMIN_GROUP_ID || null;
const ADMIN_STATE_FILE_PATH = './admin-state.json';

const SYSTEM_PROMPT = `Ты - виртуальный помощник Кристина УК "Прогресс".

Твои задачи:
- Консультировать по услугам, графику, контактам.
- Принимать заявки на ремонт: перед приемом заявки узнай как можно больше информации, например: проблема во всем доме или в одной квартире? уточни адрес и время, подтверди прием заявки.
- Помогать с оплатой.
- Предоставлять номера лицевых счетов жильцам для входа в приложение (нужны ФИО и точный адрес).
- Фиксировать жалобы.

КОГДА ЖИЛЕЦ ПРОСИТ ЛИЦЕВОЙ СЧЕТ:
- Естественно попроси полное ФИО (фамилия и имя достаточно)
- Попроси точный адрес (улица, дом, квартира)
- Собирай информацию постепенно в ходе беседы
- Когда у тебя есть полное ФИО и адрес, добавь в свой ответ слово LOOKUP_ACCOUNT чтобы система могла найти счет
- Если предоставленная информация не подходит, вежливо попроси уточнить данные
- При неудаче предложи обратиться в офис

Справочная информация:
- График: Пн-Пт, 9:00-18:00.
- Адрес: Ирчи Казака 31.
- Офис: +7 (800) 444-52-05.
- Контакты юриста: +7 (929) 867-91-90.
- Оплата: Переводом на номер: +7 (900) 050 11 11, в офисе или через приложение УК «Прогресс».
  - iOS: https://apps.apple.com/app/id6738488843
  - Android: https://play.google.com/store/apps/details?id=ru.burmistr.app.client.c_4296

Важно:
- Будь профессиональной и четкой. Избегай излишней эмпатии и фраз вроде \"Мы понимаем ваше расстройство\", но продолжай быть вежливой и на фразы "Спасибо" или "До свидания" - отвечай так же тепло.
- Отвечай кратко и по делу. Не предлагай свою помощь каждый раз. Если жильцу нужна помощь - он сам обратится.
- Ссылки отправляй как обычный текст, без форматирования.
- Говори только на русском.
- Не придумывай, если не знаешь ответ.
- Представляйся как виртуальный помощник, а не живой сотрудник.

Цель: быстро помочь и оставить приятное впечатление.`;

// --- INITIALIZATION ---
const openai = new OpenAI({
    apiKey: process.env.OPENAI_API_KEY,
});

const client = new Client({
    authStrategy: new LocalAuth()
});

let conversationHistories = {};
let mutedChats = {}; // { [chatId]: { until: number | null } }
let excelParser = null;

// --- MESSAGE DEBOUNCING ---
const MESSAGE_DEBOUNCE_WAIT = 1 * 10 * 1000; // 2 minutes in milliseconds
let messageBuffers = {}; // Store pending messages for each chat
let messageDebouncers = {}; // Store debouncer instances for each chat

// --- TYPING HELPERS (context7 timings) ---
function calculateTypingDurationMs(text) {
    const msPerChar = 70;
    const minMs = 700;
    const maxMs = 7000;
    const length = typeof text === 'string' ? text.length : 0;
    return Math.max(minMs, Math.min(maxMs, length * msPerChar));
}

async function showTypingForDuration(chat, durationMs) {
    try {
        if (!chat) return;
        // Immediately show typing, then keep alive every ~2s
        chat.sendStateTyping();
        const intervalId = setInterval(() => {
            chat.sendStateTyping();
        }, 2000);
        await new Promise(resolve => setTimeout(resolve, durationMs));
        clearInterval(intervalId);
        await chat.clearState();
    } catch (e) {
        // Ignore typing errors; proceed to send
    }
}

// Format bot messages with italics and prefix
function formatBotMessage(text) {
    // Split text into lines and apply italics to each non-empty line
    const lines = text.split('\n');
    const formattedLines = lines.map(line => {
        // If line is empty or just whitespace, keep it as is
        if (line.trim() === '') {
            return line;
        }
        // Trim whitespace and apply italics to non-empty lines
        return `_${line.trim()}_`;
    });
    
    return `*[Bot Кристина]:*\n${formattedLines.join('\n')}`;
}

async function sendReplyWithTyping(message, text) {
    try {
        const chat = await message.getChat();
        if (chat && !chat.isGroup) {
            const delayMs = calculateTypingDurationMs(text);
            await showTypingForDuration(chat, delayMs);
        }
        const formattedText = formatBotMessage(text);
        
        // Track this as bot's automated response
        const targetChatId = message.from;
        const messageKey = `${targetChatId}:${formattedText}`;
        botAutomatedResponses.add(messageKey);
        
        await message.reply(formattedText);
    } catch (e) {
        try { 
            const formattedText = formatBotMessage(text);
            
            // Track this as bot's automated response
            const targetChatId = message.from;
            const messageKey = `${targetChatId}:${formattedText}`;
            botAutomatedResponses.add(messageKey);
            
            await message.reply(formattedText); 
        } catch (_) {}
    }
}

async function sendMessageWithTyping(chatId, text) {
    try {
        const chat = await client.getChatById(chatId);
        if (chat && !chat.isGroup) {
            const delayMs = calculateTypingDurationMs(text);
            await showTypingForDuration(chat, delayMs);
        }
        const formattedText = formatBotMessage(text);
        
        // Track this as bot's automated response
        const messageKey = `${chatId}:${formattedText}`;
        botAutomatedResponses.add(messageKey);
        
        await client.sendMessage(chatId, formattedText);
    } catch (e) {
        try { 
            const formattedText = formatBotMessage(text);
            
            // Track this as bot's automated response
            const messageKey = `${chatId}:${formattedText}`;
            botAutomatedResponses.add(messageKey);
            
            await client.sendMessage(chatId, formattedText); 
        } catch (_) {}
    }
}

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


function getRecentUserMessages(history, limit = 5) {
    const result = [];
    for (let i = history.length - 1; i >= 0 && result.length < limit; i--) {
        const msg = history[i];
        if (msg.role !== 'user') continue;
        if (typeof msg.content === 'string') {
            if (msg.content.trim()) result.push(msg.content.trim());
        } else if (Array.isArray(msg.content)) {
            const textPart = msg.content.find(p => p.type === 'text');
            if (textPart && textPart.text && textPart.text.trim()) result.push(textPart.text.trim());
        }
    }
    return result.reverse();
}


// --- ADMIN STATE & COMMANDS ---
function normalizePhone(phone) {
    return String(phone || '').replace(/[^\d]/g, '');
}

function phoneToChatId(phone) {
    const digits = normalizePhone(phone);
    return digits ? `${digits}@c.us` : null;
}

function parseDuration(text) {
    if (!text) return null;
    const match = String(text).trim().match(/^(\d+)\s*(s|m|h|d)$/i);
    if (!match) return null;
    const amount = parseInt(match[1], 10);
    const unit = match[2].toLowerCase();
    const multiplier = { s: 1000, m: 60000, h: 3600000, d: 86400000 }[unit];
    return amount * multiplier;
}

async function saveAdminState() {
    try {
        await fs.writeFile(ADMIN_STATE_FILE_PATH, JSON.stringify({ mutedChats }, null, 2), 'utf8');
    } catch (e) {
        console.error('Error saving admin state:', e);
    }
}

async function loadAdminState() {
    try {
        await fs.access(ADMIN_STATE_FILE_PATH);
        const data = await fs.readFile(ADMIN_STATE_FILE_PATH, 'utf8');
        const parsed = JSON.parse(data);
        mutedChats = parsed.mutedChats || {};
        console.log('Successfully loaded admin state.');
    } catch (e) {
        if (e.code === 'ENOENT') {
            console.log('No admin state file found. Starting with empty admin state.');
        } else {
            console.error('Error loading admin state:', e);
        }
    }
}

function isChatMuted(chatId) {
    const entry = mutedChats[chatId];
    if (!entry) return false;
    if (entry.until && Date.now() > entry.until) {
        delete mutedChats[chatId];
        saveAdminState();
        return false;
    }
    return true;
}

async function muteChat(chatId, durationMs) {
    const until = durationMs ? Date.now() + durationMs : null;
    mutedChats[chatId] = { until };
    // Cancel any pending debounced processing and clear buffer
    if (messageDebouncers[chatId]) {
        try { messageDebouncers[chatId].cancel(); } catch (_) {}
    }
    messageBuffers[chatId] = [];
    await saveAdminState();
}

async function unmuteChat(chatId) {
    delete mutedChats[chatId];
    await saveAdminState();
}

function formatMute(entry) {
    if (!entry) return '🔔 Включен';
    if (!entry.until) return '🔕 Отключен • бессрочно';
    const remainingMs = entry.until - Date.now();
    if (remainingMs <= 0) return '🔔 Включен';
    return `🔕 Отключен • ещё ${formatDurationShort(remainingMs)} (до ${formatDateTime(entry.until)})`;
}

async function handleAdminCommand(message) {
    const raw = (message.body || '').trim();
    const [cmdRaw, phoneArg, durArg] = raw.split(/\s+/);
    const cmd = (cmdRaw || '').toLowerCase();
    const replyTarget = message.fromMe ? (message.to || message.from) : message.from;

    if (cmd === '!mute') {
        if (!phoneArg) { await client.sendMessage(replyTarget, 'Использование: !mute <телефон> [30m|2h|1d]'); return; }
        const chatId = phoneToChatId(phoneArg);
        if (!chatId) { await client.sendMessage(replyTarget, 'Некорректный телефон.'); return; }
        const ms = parseDuration(durArg);
        await muteChat(chatId, ms);
        const untilTs = mutedChats[chatId].until;
        const timeInfo = untilTs ? `до ${formatDateTime(untilTs)} (ещё ${formatDurationShort(untilTs - Date.now())})` : 'бессрочно';
        await client.sendMessage(replyTarget, `✅ Отключила AI ответы для ${chatId}\n⏳ Срок: ${timeInfo}`);
        return;
    }
    if (cmd === '!unmute') {
        if (!phoneArg) { await client.sendMessage(replyTarget, 'Использование: !unmute <телефон>'); return; }
        const chatId = phoneToChatId(phoneArg);
        if (!chatId) { await client.sendMessage(replyTarget, 'Некорректный телефон.'); return; }
        await unmuteChat(chatId);
        await client.sendMessage(replyTarget, `✅ Включила AI ответы для ${chatId}`);
        return;
    }
    if (cmd === '!status') {
        cleanupExpiredMutes();
        if (phoneArg) {
            const chatId = phoneToChatId(phoneArg);
            const state = mutedChats[chatId];
            await client.sendMessage(replyTarget, `${chatId}: ${formatMute(state)}`);
        } else {
            const now = Date.now();
            const entries = Object.entries(mutedChats).filter(([_, e]) => !e.until || e.until > now);
            if (!entries.length) { await client.sendMessage(replyTarget, '✅ Сейчас нет отключенных чатов.'); return; }
            const lines = entries.map(([id, e]) => `• ${id}: ${formatMute(e)}`);
            await client.sendMessage(replyTarget, `🧾 Отключенные чаты (${entries.length}):\n${lines.join('\n')}`);
        }
        return;
    }
    if (cmd === '!help') {
        await client.sendMessage(replyTarget, 'Команды:\n• !mute <телефон> [30m|2h|1d]\n• !unmute <телефон>\n• !status [телефон]');
        return;
    }
    await client.sendMessage(replyTarget, 'Неизвестная команда. !help');
}

function cleanupExpiredMutes() {
    const now = Date.now();
    let changed = false;
    for (const [id, e] of Object.entries(mutedChats)) {
        if (e && e.until && now > e.until) {
            delete mutedChats[id];
            changed = true;
        }
    }
    if (changed) {
        saveAdminState();
    }
}

function formatDurationShort(ms) {
    const s = Math.max(0, Math.floor(ms / 1000));
    const d = Math.floor(s / 86400);
    const h = Math.floor((s % 86400) / 3600);
    const m = Math.floor((s % 3600) / 60);
    if (d > 0) {
        if (h > 0) return `${d} д ${h} ч`;
        return `${d} д`;
    }
    if (h > 0) {
        if (m > 0) return `${h} ч ${m} мин`;
        return `${h} ч`;
    }
    return `${Math.max(1, m)} мин`;
}

function formatDateTime(ts) {
    try {
        return new Date(ts).toLocaleString('ru-RU');
    } catch (_) {
        return new Date(ts).toLocaleString();
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

// --- VIDEO HANDLING FUNCTION ---
async function handleVideo(media) {
    try {
        console.log("Received video, extracting frames for analysis...");
        const videoBuffer = Buffer.from(media.data, 'base64');
        const tempVideoPath = `./temp_video_${Date.now()}.mp4`;
        const framesDir = `./temp_frames_${Date.now()}`;
        
        // Save video to temporary file
        await fs.writeFile(tempVideoPath, videoBuffer);
        
        // Create frames directory
        await fs.mkdir(framesDir, { recursive: true });
        
        // Extract frames using ffmpeg (every 2 seconds to avoid too many frames)
        await new Promise((resolve, reject) => {
            ffmpeg(tempVideoPath)
                .outputOptions([
                    '-vf fps=0.5', // Extract 1 frame every 2 seconds
                    '-vframes 10'   // Limit to 10 frames max
                ])
                .output(path.join(framesDir, 'frame_%03d.jpg'))
                .on('end', resolve)
                .on('error', reject)
                .run();
        });
        
        // Read extracted frames
        const frameFiles = await fs.readdir(framesDir);
        const base64Frames = [];
        
        for (const frameFile of frameFiles.sort()) {
            if (frameFile.endsWith('.jpg')) {
                const framePath = path.join(framesDir, frameFile);
                const frameBuffer = await fs.readFile(framePath);
                const base64Frame = frameBuffer.toString('base64');
                base64Frames.push(base64Frame);
            }
        }
        
        // Clean up temporary files
        await fs.unlink(tempVideoPath);
        await fs.rm(framesDir, { recursive: true, force: true });
        
        console.log(`Extracted ${base64Frames.length} frames from video`);
        
        // Create OpenAI content with frames
        const openAIContent = [
            { type: 'text', text: 'Пользователь отправил видео. Проанализируй содержимое видео и опиши что на нем происходит. Отвечай на русском языке.' },
            ...base64Frames.map(frame => ({
                type: 'image_url',
                image_url: { url: `data:image/jpeg;base64,${frame}` }
            }))
        ];
        
        return openAIContent;
        
    } catch (error) {
        console.error("Error processing video:", error);
        return "Не могу обработать видео. Пожалуйста, попробуйте отправить его еще раз или опишите проблему текстом.";
    }
}

// --- MESSAGE BATCHING FUNCTIONS ---
async function processBatchedMessages(chatId) {
    const messages = messageBuffers[chatId];
    if (!messages || messages.length === 0) {
        return;
    }

    console.log(`Processing ${messages.length} batched messages for ${chatId}`);
    
    const history = conversationHistories[chatId] || [];
    
    // Add all buffered messages to history
    for (const messageData of messages) {
        history.push(messageData.userHistoryEntry);
    }
    
    try {
        
        // Create a combined context for the AI that mentions multiple messages
        const combinedContext = messages.length > 1 
            ? `The user has sent ${messages.length} messages in sequence. Please analyze them as a whole and provide a comprehensive response.`
            : '';
        
        // Add context if multiple messages
        if (combinedContext) {
            history.push({ role: "system", type: 'text', content: combinedContext });
        }
        
        const aiResponse = await getOpenAIResponse(history);
        
        // Remove the system context message if we added one
        if (combinedContext) {
            history.pop();
        }

        // Check if AI response indicates it wants to look up an account
        if (aiResponse.includes('LOOKUP_ACCOUNT')) {
            const accountHandled = await handleAccountLookup(chatId, history);
            if (accountHandled) {
                conversationHistories[chatId] = history;
                await saveHistory();
                // Clear the buffer
                messageBuffers[chatId] = [];
                return;
            }
            // Remove the LOOKUP_ACCOUNT keyword from the response
            const cleanedResponse = aiResponse.replace('LOOKUP_ACCOUNT', '').trim();
            history.push({ role: "assistant", type: 'text', content: cleanedResponse });
            conversationHistories[chatId] = history;
            
            // Reply to the last message in the batch
            const lastMessage = messages[messages.length - 1].originalMessage;
            await sendReplyWithTyping(lastMessage, cleanedResponse);
            await saveHistory();
        } else {
            // Normal reply
            history.push({ role: "assistant", type: 'text', content: aiResponse });
            conversationHistories[chatId] = history;
            const lastMessage = messages[messages.length - 1].originalMessage;
            await sendReplyWithTyping(lastMessage, aiResponse);
            await saveHistory();
        }

        if (history.length > MAX_HISTORY_LENGTH) {
            console.log(`History for ${chatId} exceeds limit. Triggering summarization.`);
            await summarizeHistory(chatId);
            await saveHistory();
        }

    } catch (error) {
        console.error("Error processing batched messages:", error);
        // Reply to the last message in the batch with error
        const lastMessage = messages[messages.length - 1].originalMessage;
        await sendReplyWithTyping(lastMessage, "Проблемы с WhatsApp. Просим обратиться по номеру: +7 (800) 444-52-05");
    }
    
    // Clear the buffer after processing
    messageBuffers[chatId] = [];
}

function getOrCreateDebouncer(chatId) {
    if (!messageDebouncers[chatId]) {
        messageDebouncers[chatId] = new Debouncer(
            () => processBatchedMessages(chatId),
            { wait: MESSAGE_DEBOUNCE_WAIT }
        );
    }
    return messageDebouncers[chatId];
}


// --- WHATSAPP CLIENT EVENTS ---
client.on('qr', qr => {
    qrcode.generate(qr, { small: true });
});

client.on('ready', () => {
    console.log('Client is ready!');
});

// Track bot's own automated responses to distinguish from live operator messages
const botAutomatedResponses = new Set();

// Clean up old automated response tracking entries every 5 minutes
setInterval(() => {
    // Clear all entries - they should be processed within seconds anyway
    botAutomatedResponses.clear();
    console.log('Cleaned up automated response tracking');
}, 5 * 60 * 1000);

// Detect when live operator sends message from bot account and cancel pending bot responses
client.on('message_create', async (message) => {
    try {
        // If this is a message sent by the bot (live operator using bot account)
        if (message.fromMe) {
            const targetChatId = message.to || message.from;
            const messageContent = message.body || '';
            
            // Handle admin commands in admin group
            if (ADMIN_GROUP_ID && targetChatId === ADMIN_GROUP_ID) {
                const body = messageContent.trim();
                if (body.startsWith('!')) {
                    await handleAdminCommand(message);
                }
                return;
            }
            
            // If this is a message to a regular user (not group, not admin group)
            if (!targetChatId.endsWith('@g.us') && targetChatId !== ADMIN_GROUP_ID) {
                // Check if this is a bot's own automated response
                const messageKey = `${targetChatId}:${messageContent}`;
                if (botAutomatedResponses.has(messageKey)) {
                    // This is the bot's own automated response, remove from tracking and ignore
                    botAutomatedResponses.delete(messageKey);
                    return;
                }
                
                // This is a live operator message - cancel pending bot responses
                console.log(`Live operator sent message to ${targetChatId}, canceling pending bot response`);
                
                // Cancel any pending bot response for this user
                if (messageDebouncers[targetChatId]) {
                    messageDebouncers[targetChatId].cancel();
                    console.log(`Canceled pending bot response for ${targetChatId}`);
                }
                
                // Clear message buffer for this user
                if (messageBuffers[targetChatId]) {
                    messageBuffers[targetChatId] = [];
                    console.log(`Cleared message buffer for ${targetChatId}`);
                }
                
                // Add the live operator's message to conversation history
                if (!conversationHistories[targetChatId]) {
                    conversationHistories[targetChatId] = [];
                }
                
                conversationHistories[targetChatId].push({
                    role: "assistant",
                    type: 'text',
                    content: messageContent,
                    timestamp: Date.now(),
                    isLiveOperator: true
                });
                
                await saveHistory();
            }
        }
    } catch (e) {
        console.error('Live operator detection error:', e);
     }
 });

client.on('message', async message => {
    if (message.isStatus) return;
    // Ignore our own outgoing messages to prevent self-triggering on broadcasts/mailing
    if (message.fromMe) return;

    // Route admin group commands before generic group handling
    if (ADMIN_GROUP_ID && message.from === ADMIN_GROUP_ID) {
        try {
            const body = (message.body || '').trim();
            if (body.startsWith('!')) {
                await handleAdminCommand(message);
            }
        } catch (e) {
            console.error('Admin command error:', e);
        }
        return;
    }

    if (message.from.endsWith('@g.us')) {
        console.log(`Message received from group: ${message.from}`);
        return;
    }

    let messageBody = message.body;
    let userHistoryEntry;

    // Handle special commands immediately (no debouncing)
    if (messageBody.toLowerCase() === '!reset') {
        delete conversationHistories[message.from];
        // Also clear any pending messages and cancel debouncer
        messageBuffers[message.from] = [];
        if (messageDebouncers[message.from]) {
            messageDebouncers[message.from].cancel();
        }
        await saveHistory();
        console.log(`History for ${message.from} has been reset.`);
        await sendReplyWithTyping(message, "I've cleared our previous conversation. Let's start fresh.");
        return;
    }

    try {
        // Process media and create user history entry
        if (message.hasMedia) {
            const media = await message.downloadMedia();
            if (media.mimetype === 'image/jpeg' || media.mimetype === 'image/png' || media.mimetype === 'image/webp') {
                try {
                    console.log("Received image message, adding to batch...");
                    const openAIContent = [
                        { type: 'text', text: message.body },
                        { type: 'image_url', image_url: { url: `data:${media.mimetype};base64,${media.data}` } }
                    ];
                    userHistoryEntry = { role: "user", type: 'image', content: openAIContent, media: { mimetype: media.mimetype, data: media.data } };
                } catch (error) {
                    console.error("Error processing image:", error);
                    await sendReplyWithTyping(message, "Сейчас не могу открыть фото. Пожалуйста, напишите.");
                    return;
                }
            } else if (media.mimetype === 'application/pdf') {
                try {
                    console.log("Received PDF message, adding to batch...");
                    messageBody = await handlePdf(media);
                    userHistoryEntry = { role: "user", type: 'file', content: messageBody, media: { mimetype: media.mimetype, data: media.data, filename: media.filename } };
                } catch (error) {
                    console.error("Error processing PDF:", error);
                    await sendReplyWithTyping(message, "Сейчас не могу открыть файл. Пожалуйста, напишите.");
                    return;
                }
            } else if (media.mimetype === 'audio/ogg' || message.type === 'ptt' || message.type === 'audio') {
                try {
                    console.log("Received voice message, transcribing and adding to batch...");
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
                    await sendReplyWithTyping(message, "Не разобрала ваше голосовое. Пожалуйста, напишите.");
                    return;
                }
            } else if (media.mimetype === 'video/mp4' || media.mimetype === 'video/quicktime' || media.mimetype === 'video/avi' || media.mimetype === 'video/mov' || media.mimetype === 'video/webm') {
                try {
                    console.log("Received video message, processing and adding to batch...");
                    const openAIContent = await handleVideo(media);
                    if (typeof openAIContent === 'string') {
                        // Error case
                        await sendReplyWithTyping(message, openAIContent);
                        return;
                    }
                    userHistoryEntry = { role: "user", type: 'video', content: openAIContent, media: { mimetype: media.mimetype, data: media.data } };
                } catch (error) {
                    console.error("Error processing video:", error);
                    await sendReplyWithTyping(message, "Не могу обработать видео. Пожалуйста, попробуйте отправить его еще раз или опишите проблему текстом.");
                    return;
                }
            } else {
                // Unsupported file type - handle immediately
                console.log(`Received unsupported file type: ${media.mimetype}`);
                await sendReplyWithTyping(message, "Не могу сейчас открыть ваше вложение. Пожалуйста, напишите.");
                return;
            }
        } else {
            userHistoryEntry = { role: "user", type: 'text', content: messageBody };
        }

        // If chat is muted, store to history and do not reply/buffer
        if (isChatMuted(message.from)) {
            const history = conversationHistories[message.from] || [];
            history.push(userHistoryEntry);
            conversationHistories[message.from] = history;
            await saveHistory();
            return;
        }

        // Initialize message buffer for this chat if it doesn't exist
        if (!messageBuffers[message.from]) {
            messageBuffers[message.from] = [];
        }

        // Add message to buffer
        messageBuffers[message.from].push({
            userHistoryEntry,
            originalMessage: message,
            timestamp: Date.now()
        });

        console.log(`Message from ${message.from} added to batch (${messageBuffers[message.from].length} messages pending)`);

        // Get or create debouncer for this chat and trigger it
        const debouncer = getOrCreateDebouncer(message.from);
        debouncer.maybeExecute();

    } catch (error) {
        console.error("Error handling message:", error);
        await sendReplyWithTyping(message, "Не могу почему то открыть сообщение. Напишите пожалуйста.");
    }
});

// --- STARTUP SEQUENCE ---
async function start() {
    await loadHistory();
    await loadAdminState();
    
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

// --- ACCOUNTING REQUEST FUNCTIONS ---


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
                await sendMessageWithTyping(chatId, accountMessage);
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