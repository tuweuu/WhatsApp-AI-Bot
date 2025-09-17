const qrcode = require('qrcode-terminal');
const { Client, LocalAuth, MessageMedia } = require('whatsapp-web.js');
const { OpenAI } = require("openai");
const fs = require('fs').promises;
const fsSync = require('fs');
const pdf = require('pdf-parse');
const ExcelParser = require('./excel-parser');
const HistoryManager = require('./history-manager');
const { Debouncer } = require('@tanstack/pacer');
const ffmpeg = require('fluent-ffmpeg');
const path = require('path');
const { v4: uuidv4 } = require('uuid');
const { getCurrentBotConfig, hasAdminAccess, getSystemPrompt, getDisplayName, getClientId, isMainBot } = require('./bot-config');
const { format, isWeekend, isToday, addDays, getDay, setHours, getHours } = require('date-fns');
const { ru } = require('date-fns/locale');
require('dotenv').config();

// --- CONFIGURATION ---
const OPENAI_MODEL = "gpt-4.1";
const MAX_HISTORY_LENGTH = 50;
const SUMMARIZATION_PROMPT = "Briefly summarize your conversation with the resident. Note down key details, names, and specific requests to ensure a smooth follow-up.";

// Critical guard: confirmation is handled by our code, not the LLM
const CONFIRMATION_DELEGATION_RULES = `ВАЖНО:
— Никогда не проси пользователя подтвердить данные фразами вида «подтвердите/да или нет/если верно оформлю/передам заявку».
— Не обещай самостоятельно «оформлю/передам заявку/отправлю в бухгалтерию/администрацию».
— Подтверждение и отправка заявок выполняются системой. Твоя задача — собрать недостающие данные короткими вопросами. Если данных достаточно — не запрашивай подтверждение, отвечай по существу и жди дальнейших действий системы.`;

const ACCOUNT_EXTRACTION_PROMPT = "Analyze the ENTIRE conversation history and extract the full name and complete address for the person whose account is being requested. This could be the user themselves or someone they're asking about (like a family member). Information may be provided across multiple messages. Look for: 1) Full name (first name, last name) - may be provided in parts across different messages 2) Complete address including street name, house number, and apartment number - may also be provided in parts. Combine all address parts into a single address string. Return the data in JSON format with the keys: 'fullName' and 'address'. If any information is missing, use the value 'null'. Examples: fullName: 'Адакова Валерия Аликовна', address: 'Магомеда Гаджиева 73а, кв. 92'. Pay special attention to: - Names that may be provided as 'адакова валерия' first, then 'Адакова Валерия Аликовна' later - Addresses like 'магомед гаджиева 73а, 92кв' or 'магомед гаджиева 73а' + '92кв' separately";

// --- GROUP ROUTING INTEGRATION ---
const ADMIN_GROUP_ID = process.env.ADMIN_GROUP_ID || null;
const GENERAL_GROUP_ID = process.env.GENERAL_GROUP_ID || null;
const ACCOUNTING_GROUP_ID = process.env.ACCOUNTING_GROUP_ID || null;
const ADMIN_STATE_FILE_PATH = './admin-state.json';

// Groups to ignore - bot will never respond in these groups
const IGNORED_GROUPS = [
    '79000501111-1635839546@g.us', // Технический персонал УК «Прогресс»
    '79993100111-1562266045@g.us', // «Прогресс» | Рабочая
    '120363181424301003@g.us', // ДагЛифт | УК «Прогресс»
    '120363409741682571@g.us', // Технички Кадырова 44/46
    '120363042216780683@g.us', // Камеры/домофония
    '120363151482260621@g.us', // 👮🏼 Консьержи УК «Прогресс»
    '120363421039187370@g.us', // 🤖 [bot]: 💰 Бухгалтерия
    '120363418711369407@g.us', // 🤖 [bot]: 📑 Заявки
    '120363421860873400@g.us', // 🧾 Квитанции об оплате 💸
    '120363424059988249@g.us', // 🤖 [bot]: 👨🏻‍💻 Admin
];

// Phone numbers to ignore - bot will never respond to these numbers
const IGNORED_NUMBERS = [
    '79000501111@c.us', //Основной бот
    '79298682421@c.us', //Диспетчер 1
    '79280453783@c.us', //Диспетчер 2
    '79387900059@c.us', //Диспетчер 3
    '79288793111@c.us' //Бухгалтерия
];

function isIgnoredGroup(groupId) {
    return IGNORED_GROUPS.includes(groupId);
}

function isIgnoredNumber(chatId) {
    return IGNORED_NUMBERS.includes(chatId);
}

// Get system prompt from configuration
const SYSTEM_PROMPT = getSystemPrompt();

// --- WORKING HOURS HELPER ---
function isWorkingHours() {
    const now = new Date();
    const currentHour = getHours(now);
    const currentDay = getDay(now); // 0 = Sunday, 1 = Monday, ..., 6 = Saturday
    
    // Check if it's weekend (Saturday = 6, Sunday = 0)
    if (currentDay === 0 || currentDay === 6) {
        return false;
    }
    
    // Check if it's after 18:00 (6 PM)
    if (currentHour >= 18) {
        return false;
    }
    
    return true;
}

// --- INITIALIZATION ---
const openai = new OpenAI({
    apiKey: process.env.OPENAI_API_KEY,
});

// Get current bot configuration
const botConfig = getCurrentBotConfig();
console.log(`Starting bot instance: ${botConfig.name} (${botConfig.clientId})`);

const client = new Client({
    authStrategy: new LocalAuth({
        clientId: botConfig.clientId,
        dataPath: `./auth-sessions/${botConfig.clientId}`
    })
});

// Initialize the new history manager
const historyManager = new HistoryManager();
let mutedChats = {}; // { [chatId]: { until: number | null } }
let excelParser = null;

// --- MESSAGE DEBOUNCING ---
const MESSAGE_DEBOUNCE_WAIT = 1 * 10 * 1000; // 2 minutes in milliseconds
let messageBuffers = {}; // Store pending messages for each chat
let messageDebouncers = {}; // Store debouncer instances for each chat
let groupMessageBuffers = {}; // Store pending group messages for each group
let groupMessageDebouncers = {}; // Store debouncer instances for each group
let pendingRequests = {}; // Store pending requests waiting for confirmation
let residentDataCache = {}; // Cache extracted resident data to avoid re-asking
let botStartTime = null; // Track when the bot started to ignore old messages

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
    
    return `*[${getDisplayName()}]:*\n${formattedLines.join('\n')}`;
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

// --- DATE/TIME AWARENESS FUNCTIONS ---
function getCurrentDateTimeContext() {
    const now = new Date();
    const today = format(now, 'EEEE, d MMMM yyyy', { locale: ru });
    const currentTime = format(now, 'HH:mm', { locale: ru });
    const dayOfWeek = getDay(now); // 0 = Sunday, 1 = Monday, etc.
    const isCurrentWeekend = isWeekend(now);
    
    // Check if tomorrow is weekend
    const tomorrow = addDays(now, 1);
    const isTomorrowWeekend = isWeekend(tomorrow);
    const tomorrowName = format(tomorrow, 'EEEE', { locale: ru });
    
    // Determine if it's working hours (9 AM to 6 PM, Monday to Friday)
    const currentHour = getHours(now);
    const isWorkingDay = !isCurrentWeekend;
    const isWorkingHours = isWorkingDay && currentHour >= 9 && currentHour < 18;
    
    let context = `ТЕКУЩАЯ ДАТА И ВРЕМЯ:
`;
    context += `Сегодня: ${today}
`;
    context += `Время: ${currentTime}
`;
    context += `Завтра: ${tomorrowName}`;
    
    if (isCurrentWeekend) {
        context += `\nСегодня выходной день (суббота/воскресенье)`;
    }
    
    if (isTomorrowWeekend) {
        context += `\nЗавтра выходной день (суббота/воскресенье)`;
    }
    
    if (isWorkingHours) {
        context += `\nСейчас рабочее время (9:00-18:00, понедельник-пятница)`;
    } else if (isWorkingDay) {
        context += `\nСейчас нерабочее время (рабочие часы: 9:00-18:00)`;
    }
    
    context += `\n\nИСПОЛЬЗУЙ ЭТУ ИНФОРМАЦИЮ для ответов на вопросы о времени, рабочих днях, выходных, "завтра", "сегодня", "сейчас" и т.д.`;
    
    return context;
}

// --- PERSISTENCE FUNCTIONS ---
// Legacy functions for backward compatibility - now use HistoryManager
async function saveHistory(chatId = null) {
    // If chatId is provided, save only that chat
    if (chatId) {
        await historyManager.saveChatHistory(chatId);
    } else {
        // Save all chats in memory (for backward compatibility)
        const chatIds = historyManager.getAllChatIds();
        if (chatIds && Array.isArray(chatIds)) {
            for (const id of chatIds) {
                await historyManager.saveChatHistory(id);
            }
        }
    }
}

async function loadHistory() {
    // Initialize the history manager and migrate if needed
    await historyManager.initialize();
    const migrationResult = await historyManager.migrateFromOldFormat();
    if (migrationResult.success && migrationResult.migratedCount > 0) {
        console.log(`Migration completed: ${migrationResult.migratedCount} chats migrated`);
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
    // Check if current bot instance has admin access
    if (!hasAdminAccess()) {
        const replyTarget = message.fromMe ? (message.to || message.from) : message.from;
        await client.sendMessage(replyTarget, 'У вас нет доступа к административным командам.');
        return;
    }
    
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
        const displayPhone = chatId.replace('@c.us', '');
        await client.sendMessage(replyTarget, `✅ Отключила AI ответы для ${displayPhone}\n⏳ Срок: ${timeInfo}`);
        return;
    }
    if (cmd === '!unmute') {
        if (!phoneArg) { await client.sendMessage(replyTarget, 'Использование: !unmute <телефон>'); return; }
        const chatId = phoneToChatId(phoneArg);
        if (!chatId) { await client.sendMessage(replyTarget, 'Некорректный телефон.'); return; }
        await unmuteChat(chatId);
        const displayPhone = chatId.replace('@c.us', '');
        await client.sendMessage(replyTarget, `✅ Включила AI ответы для ${displayPhone}`);
        return;
    }
    if (cmd === '!status') {
        cleanupExpiredMutes();
        if (phoneArg) {
            const chatId = phoneToChatId(phoneArg);
            const state = mutedChats[chatId];
            const displayPhone = chatId.replace('@c.us', '');
            await client.sendMessage(replyTarget, `${displayPhone}: ${formatMute(state)}`);
        } else {
            const now = Date.now();
            const entries = Object.entries(mutedChats).filter(([_, e]) => !e.until || e.until > now);
            if (!entries.length) { await client.sendMessage(replyTarget, '✅ Сейчас нет отключенных чатов.'); return; }
            const lines = entries.map(([id, e]) => `• ${id.replace('@c.us', '')}: ${formatMute(e)}`);
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

// --- FILE SENDING FUNCTION ---
/**
 * Sends a file from message history back to the user
 * @param {Object} message - The original WhatsApp message
 * @param {Object} historyEntry - The history entry containing file data
 */
async function sendFileFromHistory(message, historyEntry) {
    try {
        if (historyEntry.type === 'file' && historyEntry.media) {
            const { mimetype, filename } = historyEntry.media;
            const fileInfo = `📄 File: ${filename || 'document'} (${mimetype})\n\nContent: ${historyEntry.content}`;
            await sendReplyWithTyping(message, fileInfo);
        }
    } catch (error) {
        console.error('Error sending file from history:', error);
        await sendReplyWithTyping(message, 'Could not resend the file.');
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

// --- PAYMENT DATA EXTRACTION FUNCTION ---
async function extractPaymentData(pdfContent, filename) {
    const PAYMENT_EXTRACTION_PROMPT = `Analyze the PDF content and extract payment information. Look for:
1. Sender full name (who is sending the payment) - could be "отправитель", "плательщик", "от кого"
2. Recipient full name (who is receiving the payment) - could be "получатель", "в пользу", "кому"  
3. Amount sent - the payment amount with currency

PDF filename: ${filename || 'Unknown'}
PDF content: ${pdfContent}

Return JSON with keys: 'senderName', 'recipientName', 'amount'
If any information is not found, use 'Не указано' for that field.
For amount, include currency if mentioned (e.g., "1500 руб" or "25000₽")

Examples:
- senderName: "Иванов Иван Петрович"
- recipientName: "ООО Управляющая Компания Прогресс"  
- amount: "15000 руб"`;

    try {
        const completion = await openai.chat.completions.create({
            model: OPENAI_MODEL,
            messages: [
                { role: "system", content: PAYMENT_EXTRACTION_PROMPT },
                { role: "user", content: `Extract payment data from this document.` }
            ],
            response_format: { type: "json_object" },
            max_tokens: 200
        });
        
        const extractedData = JSON.parse(completion.choices[0].message.content);
        return {
            senderName: extractedData.senderName || 'Не указано',
            recipientName: extractedData.recipientName || 'Не указано',
            amount: extractedData.amount || 'Не указано'
        };
    } catch (error) {
        console.error('Error extracting payment data:', error);
        return {
            senderName: 'Не указано',
            recipientName: 'Не указано',
            amount: 'Не указано'
        };
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
    
    const history = await historyManager.getHistory(chatId);
    
    // Note: User messages are already saved to history immediately when received
    // So we don't need to add them to history again here
    
    try {
        
        // Create a combined context for the AI that mentions multiple messages
        const combinedContext = messages.length > 1 
            ? `The user has sent ${messages.length} messages in sequence. Please analyze them as a whole and provide a comprehensive response.`
            : '';
        
        // Add context if multiple messages
        if (combinedContext) {
            history.push({ role: "system", type: 'text', content: combinedContext });
        }
        
        // Analyze if request should be routed to a group
        const routingType = await analyzeRequestForRouting(history);
        
        if (routingType && routingType !== 'NONE') {
            // Check if request has sufficient information before routing
            const completenessAnalysis = await analyzeRequestCompleteness(history, routingType);
            
            if (!completenessAnalysis.complete) {
                // Remove the system context message if we added one
                if (combinedContext) {
                    history.pop();
                }
                
                // Ask clarifying questions instead of routing
                const clarifyingMessage = completenessAnalysis.clarifyingQuestions.length > 0 
                    ? completenessAnalysis.clarifyingQuestions.join(' ') 
                    : 'Для обработки вашего запроса мне нужна дополнительная информация. Можете предоставить более подробные сведения?';
                
                history.push({ role: "assistant", type: 'text', content: clarifyingMessage });
                await historyManager.updateHistory(chatId, history);
                
                const lastMessage = messages[messages.length - 1].originalMessage;
                await sendReplyWithTyping(lastMessage, clarifyingMessage);
                
                // Clear the buffer
                messageBuffers[chatId] = [];
                return;
            }
            
            // Format request data but don't send yet - show for confirmation first
            const requestData = await formatRequestForGroup(history, chatId, routingType);
            
            // Store the pending request
            pendingRequests[chatId] = {
                requestData,
                routingType,
                history: [...history], // Make a copy
                combinedContext,
                timestamp: Date.now()
            };
            
            // Remove the system context message if we added one
            if (combinedContext) {
                history.pop();
            }
            
            // Show collected data for confirmation
            const confirmationMessage = await formatConfirmationMessage(requestData, routingType, history);
            
            history.push({ role: "assistant", type: 'text', content: confirmationMessage });
            await historyManager.updateHistory(chatId, history);
            
            const lastMessage = messages[messages.length - 1].originalMessage;
            await sendReplyWithTyping(lastMessage, confirmationMessage);
            
            // Clear the buffer
            messageBuffers[chatId] = [];
            return;
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
                await historyManager.updateHistory(chatId, history);
                // Clear the buffer
                messageBuffers[chatId] = [];
                return;
            }
            // Remove the LOOKUP_ACCOUNT keyword from the response
            const cleanedResponse = aiResponse.replace('LOOKUP_ACCOUNT', '').trim();
            history.push({ role: "assistant", type: 'text', content: cleanedResponse });
            await historyManager.updateHistory(chatId, history);
            
            // Reply to the last message in the batch
            const lastMessage = messages[messages.length - 1].originalMessage;
            await sendReplyWithTyping(lastMessage, cleanedResponse);
        } else {
            // Normal reply
            history.push({ role: "assistant", type: 'text', content: aiResponse });
            await historyManager.updateHistory(chatId, history);
            const lastMessage = messages[messages.length - 1].originalMessage;
            await sendReplyWithTyping(lastMessage, aiResponse);
        }

        // Check if summarization is needed after processing the batch
        const currentHistory = await historyManager.getHistory(chatId);
        if (currentHistory.length === MAX_HISTORY_LENGTH + 1) {
            console.log(`History for ${chatId} reached limit (${MAX_HISTORY_LENGTH} + 1 = ${currentHistory.length}). Triggering summarization.`);
            await summarizeHistory(chatId);
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
    const currentBot = getCurrentBotConfig();
    const scanMessage = `=== SCAN FOR ${currentBot.name.toUpperCase()} ===`;
    console.log(`\n${scanMessage}\n`);
    qrcode.generate(qr, { small: true });
    console.log(`\n${scanMessage}\n`);
});

client.on('ready', async () => {
    console.log('Client is ready!');
    botStartTime = Date.now();
    console.log(`Bot start time recorded: ${new Date(botStartTime).toLocaleString()}`);
    
    // Log all groups the bot is part of
    try {
        const chats = await client.getChats();
        const groups = chats.filter(chat => chat.isGroup && !chat.id._serialized.includes('@broadcast'));
        
        console.log(`\n=== BOT IS MEMBER OF ${groups.length} GROUPS ===`);
        groups.forEach((group, index) => {
            console.log(`${index + 1}. ${group.name} (${group.id._serialized})`);
        });
        console.log('=====================================\n');
    } catch (error) {
        console.error('Error getting group list:', error);
    }
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
            }
            
            // If this is a message to a group, cancel pending group responses
            if (targetChatId.endsWith('@g.us')) {
                // Cancel any pending group response
                if (groupMessageDebouncers[targetChatId]) {
                    groupMessageDebouncers[targetChatId].cancel();
                    console.log(`Canceled pending group response for ${targetChatId}`);
                }
                
                // Clear group message buffer
                if (groupMessageBuffers[targetChatId]) {
                    groupMessageBuffers[targetChatId] = [];
                    console.log(`Cleared group message buffer for ${targetChatId}`);
                }
            }
            
            // Add the live operator's message to conversation history for private chats only
            if (!targetChatId.endsWith('@g.us') && targetChatId !== ADMIN_GROUP_ID) {
                await historyManager.addMessage(targetChatId, {
                    role: "assistant",
                    type: 'text',
                    content: messageContent,
                    timestamp: Date.now(),
                    isLiveOperator: true
                });
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

    // Ignore messages that were sent before bot started to prevent spam on startup
    if (botStartTime && message.timestamp * 1000 < botStartTime) {
        console.log(`Ignoring old message from ${message.from} (sent before bot start)`);
        return;
    }

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
        await handleGroupMessage(message);
        return;
    }

    // Check if this number should be ignored
    if (isIgnoredNumber(message.from)) {
        console.log(`Ignoring message from ignored number: ${message.from}`);
        return;
    }

    let messageBody = message.body;
    let userHistoryEntry;

    // Handle special commands immediately (no debouncing)
    if (messageBody.toLowerCase() === '!reset') {
        await historyManager.deleteHistory(message.from);
        // Also clear any pending messages and cancel debouncer
        messageBuffers[message.from] = [];
        if (messageDebouncers[message.from]) {
            messageDebouncers[message.from].cancel();
        }
        // Clear any pending request confirmations and cached data
        delete pendingRequests[message.from];
        delete residentDataCache[message.from];
        await saveHistory();
        console.log(`History for ${message.from} has been reset.`);
        await sendReplyWithTyping(message, "I've cleared our previous conversation. Let's start fresh.");
        return;
    }
    
    // Check if this is a confirmation response to a pending request
    if (pendingRequests[message.from]) {
        const confirmation = await analyzeConfirmationResponse(messageBody);
        if (confirmation) {
            const processed = await processConfirmationResponse(message.from, confirmation, message);
            if (processed) {
                return; // Confirmation was processed, don't continue with normal flow
            }
        } else {
            // Check if user changed topics or ignored confirmation
            const topicChange = await detectTopicChange(messageBody);
            if (topicChange) {
                console.log(`Topic change detected for ${message.from}, clearing pending confirmation`);
                delete pendingRequests[message.from];
                // Continue with normal message processing
            }
        }
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
                    userHistoryEntry = { role: "user", type: 'image', content: openAIContent, media: { mimetype: media.mimetype } };
                } catch (error) {
                    console.error("Error processing image:", error);
                    await sendReplyWithTyping(message, "Сейчас не могу открыть фото. Пожалуйста, напишите.");
                    return;
                }
            } else if (media.mimetype === 'application/pdf') {
                try {
                    console.log("Received PDF message, adding to batch...");
                    messageBody = await handlePdf(media);
                    userHistoryEntry = { role: "user", type: 'file', content: messageBody, media: { mimetype: media.mimetype, filename: media.filename } };
                    
                    // Check if this is a payment file and forward to accounting group
                    if (await isPaymentFile(messageBody, media.filename)) {
                        await forwardPaymentFileToAccounting(message, media, messageBody);
                    }
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
                    userHistoryEntry = { role: "user", type: 'audio', content: messageBody, media: { mimetype: media.mimetype } };
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
                    userHistoryEntry = { role: "user", type: 'video', content: openAIContent, media: { mimetype: media.mimetype } };
                } catch (error) {
                    console.error("Error processing video:", error);
                    await sendReplyWithTyping(message, "Не могу обработать видео. Пожалуйста, попробуйте отправить его еще раз или опишите проблему текстом.");
                    return;
                }
            } else {
                // Check if unsupported file type might be a payment document
                if (await isPaymentFileByType(media.mimetype, media.filename)) {
                    console.log(`Received potential payment file: ${media.mimetype}`);
                    await forwardPaymentFileToAccounting(message, media, null); // No PDF content for non-PDF files
                    userHistoryEntry = { role: "user", type: 'file', content: `Отправлен файл платежа: ${media.filename || 'документ'}`, media: { mimetype: media.mimetype, filename: media.filename } };
                } else {
                    // Unsupported file type - handle immediately
                    console.log(`Received unsupported file type: ${media.mimetype}`);
                    await sendReplyWithTyping(message, "Не могу сейчас открыть ваше вложение. Пожалуйста, напишите.");
                    return;
                }
            }
        } else {
            userHistoryEntry = { role: "user", type: 'text', content: messageBody };
        }

        // Save user message immediately when received (before processing/debouncing)
        await historyManager.addMessage(message.from, userHistoryEntry);

        // If chat is muted, do not reply/buffer (but message is already saved above)
        if (isChatMuted(message.from)) {
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

// --- GROUP MESSAGE HANDLING ---

/**
 * Handles incoming group messages with batching/debouncing system
 * @param {Object} message - The WhatsApp message from group
 */
async function handleGroupMessage(message) {
    try {
        // Group responses disabled
        return;
        
        // Only the main bot should respond in groups
        if (!isMainBot()) {
            console.log(`Non-main bot ignoring group message from ${message.from}`);
            return;
        }

        const groupId = message.from;
        
        // Check if this group should be ignored
        if (isIgnoredGroup(groupId)) {
            console.log(`Ignoring message from ignored group: ${groupId}`);
            return;
        }

        const messageBody = message.body;
        if (!messageBody || messageBody.trim() === '') {
            return; // Ignore empty messages
        }

        // Ignore messages that were sent before bot started to prevent spam on startup
        if (botStartTime && message.timestamp * 1000 < botStartTime) {
            console.log(`Ignoring old group message from ${message.from} (sent before bot start)`);
            return;
        }
        
        // Initialize group message buffer for this group if it doesn't exist
        if (!groupMessageBuffers[groupId]) {
            groupMessageBuffers[groupId] = [];
        }

        // Add message to buffer
        groupMessageBuffers[groupId].push({
            message,
            messageBody,
            timestamp: Date.now()
        });

        console.log(`Group message from ${groupId} added to batch (${groupMessageBuffers[groupId].length} messages pending)`);

        // Get or create debouncer for this group and trigger it
        const debouncer = getOrCreateGroupDebouncer(groupId);
        debouncer.maybeExecute();

    } catch (error) {
        console.error('Error handling group message:', error);
    }
}

/**
 * Processes batched group messages for a specific group
 * @param {string} groupId - The group ID to process messages for
 */
async function processBatchedGroupMessages(groupId) {
    const messages = groupMessageBuffers[groupId];
    if (!messages || messages.length === 0) {
        return;
    }

    console.log(`Processing ${messages.length} batched group messages for ${groupId}`);
    
    try {
        // Process each message in the batch
        for (const { message, messageBody } of messages) {
            // Analyze if this message requires management company intervention
            const needsResponse = await analyzeGroupMessage(messageBody);
            
            if (needsResponse) {
                await sendGroupSmartResponse(message, needsResponse);
                // Only respond to the first message that needs a response to avoid spam
                break;
            }
        }
    } catch (error) {
        console.error("Error processing batched group messages:", error);
    }
    
    // Clear the buffer after processing
    groupMessageBuffers[groupId] = [];
}

/**
 * Gets or creates a debouncer for a specific group
 * @param {string} groupId - The group ID
 * @returns {Object} The debouncer instance
 */
function getOrCreateGroupDebouncer(groupId) {
    if (!groupMessageDebouncers[groupId]) {
        groupMessageDebouncers[groupId] = new Debouncer(
            () => processBatchedGroupMessages(groupId),
            { wait: MESSAGE_DEBOUNCE_WAIT }
        );
    }
    return groupMessageDebouncers[groupId];
}

/**
 * Analyzes if a group message relates to management company and needs response
 * @param {string} messageText - The message text to analyze
 * @returns {Promise<Object|null>} Response details or null if no response needed
 */
async function analyzeGroupMessage(messageText) {
    const GROUP_ANALYSIS_PROMPT = `Analyze this message from a residential group chat to determine if it requires management company intervention.

Message: "${messageText}"

RESPOND if the message is about:
1. UTILITIES: electricity, water, heating, gas issues
2. REPAIRS: elevators, lights, doors, windows, plumbing
3. MAINTENANCE: cleaning, security, building problems
4. EMERGENCIES: urgent safety issues, accidents
5. BILLING: payment issues, billing questions, receipt requests
6. COMPLAINTS: noise, neighbors causing problems that need official intervention

DO NOT respond to:
- Personal conversations, social chat
- Lost pets, found items
- Private neighbor disputes
- General questions not requiring management action
- Thank you messages, casual responses
- Casual communication between residents among themselves

If management intervention is needed, determine the category:
- "utilities" for water, electricity, heating, gas
- "repairs" for maintenance, elevators, building issues
- "billing" for payment, receipt, billing questions
- "emergency" for urgent safety issues
- "general" for other management-related issues

Return JSON:
{
  "needs_response": true/false,
  "category": "utilities|repairs|billing|emergency|general" or null,
  "urgency": "high|medium|low" or null,
  "summary": "brief issue description" or null
}`;

    try {
        const completion = await openai.chat.completions.create({
            model: OPENAI_MODEL,
            messages: [
                { role: "system", content: GROUP_ANALYSIS_PROMPT },
                { role: "user", content: messageText }
            ],
            response_format: { type: "json_object" },
            max_tokens: 200,
            temperature: 0.1
        });

        const analysis = JSON.parse(completion.choices[0].message.content);
        
        if (analysis.needs_response) {
            return {
                category: analysis.category || 'general',
                urgency: analysis.urgency || 'medium',
                summary: analysis.summary || 'Требуется обращение в УК'
            };
        }
        
        return null;
    } catch (error) {
        console.error('Error analyzing group message:', error);
        return null;
    }
}

/**
 * Sends appropriate response to group directing user to private chat
 * @param {Object} message - The original WhatsApp message
 * @param {Object} responseDetails - Details about the required response
 */
async function sendGroupSmartResponse(message, responseDetails) {
    const { category, urgency } = responseDetails;
    
    // Generate bot phone number for WhatsApp link
    const botNumber = process.env.BOT_PHONE_NUMBER || '79000501111';
    const whatsappLink = `https://wa.me/${botNumber}`;
    
    let responseText = '';
    
    if (urgency === 'high' || category === 'emergency') {
        responseText = `Для экстренных случаев звоните *8 (800) 444-52-05*\n\n` +
                      `Для оформления заявки с вашими данными перейдём в личный чат 📨\n\n` +
                      `👆 Нажмите: ${whatsappLink}`;
    } else {
        let categoryMessage = '';
        switch (category) {
            case 'utilities':
                categoryMessage = 'Зафиксировала сообщение по коммунальным услугам.';
                break;
            case 'repairs':
                categoryMessage = 'Зафиксировала сообщение по ремонту/обслуживанию.';
                break;
            case 'billing':
                categoryMessage = 'Зафиксировала сообщение по начислениям.';
                break;
            default:
                categoryMessage = 'Зафиксировала ваше обращение.';
        }
        
        responseText = `${categoryMessage} Чтобы не публиковать персональные данные в группе, продолжим в личных сообщениях 📨\n\n` +
                      `👆 Нажмите: ${whatsappLink}`;
    }
    
    try {
        const formattedResponse = formatBotMessage(responseText);
        await client.sendMessage(message.from, formattedResponse);
        console.log(`Smart group response sent to ${message.from} for category: ${category}`);
    } catch (error) {
        console.error('Error sending smart group response:', error);
    }
}

// --- PAYMENT FILE HANDLING ---

/**
 * Checks if a file is a payment document based on content and filename
 * @param {string} content - The extracted text content from the file
 * @param {string} filename - The filename of the document
 * @returns {Promise<boolean>} - True if this appears to be a payment file
 */
async function isPaymentFile(content, filename) {
    // Check filename for payment-related keywords
    const paymentKeywords = [
        'платеж', 'payment', 'квитанция', 'receipt', 'чек', 'оплата',
        'банк', 'bank', 'перевод', 'transfer', 'счет', 'bill',
        'коммунальные', 'utilities', 'жкх', 'услуги'
    ];
    
    const filenameCheck = filename && paymentKeywords.some(keyword => 
        filename.toLowerCase().includes(keyword.toLowerCase())
    );
    
    // Check content for payment-related terms
    const contentKeywords = [
        'платеж', 'оплата', 'перевод', 'банк', 'сбербанк', 'втб',
        'квитанция', 'чек', 'receipt', 'payment', 'коммунальные услуги',
        'жкх', 'лицевой счет', 'сумма к оплате', 'задолженность',
        'начислено', 'к доплате', 'переплата'
    ];
    
    const contentCheck = content && contentKeywords.some(keyword => 
        content.toLowerCase().includes(keyword.toLowerCase())
    );
    
    return filenameCheck || contentCheck;
}

/**
 * Checks if a file type might be a payment document based on mimetype and filename
 * @param {string} mimetype - The MIME type of the file
 * @param {string} filename - The filename of the document
 * @returns {Promise<boolean>} - True if this might be a payment file
 */
async function isPaymentFileByType(mimetype, filename) {
    // Common document types that could contain payment information
    const documentTypes = [
        'application/vnd.ms-excel',
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        'application/msword',
        'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        'text/plain',
        'image/jpeg',
        'image/png',
        'image/webp'
    ];
    
    if (!documentTypes.includes(mimetype)) {
        return false;
    }
    
    // Check filename for payment-related keywords
    const paymentKeywords = [
        'платеж', 'payment', 'квитанция', 'receipt', 'чек', 'оплата',
        'банк', 'bank', 'перевод', 'transfer', 'счет', 'bill',
        'коммунальные', 'utilities', 'жкх', 'услуги'
    ];
    
    return filename && paymentKeywords.some(keyword => 
        filename.toLowerCase().includes(keyword.toLowerCase())
    );
}

/**
 * Forwards a payment file to the accounting group with extracted payment data
 * @param {Object} message - The original WhatsApp message
 * @param {Object} media - The media object containing the file
 * @param {string} pdfContent - The extracted PDF text content (if available)
 */
async function forwardPaymentFileToAccounting(message, media, pdfContent = null) {
    if (!ACCOUNTING_GROUP_ID) {
        console.error('ACCOUNTING_GROUP_ID is not configured, cannot forward payment file');
        return;
    }
    
    try {
        // Extract contact information
        const phone = `+${message.from.split('@')[0]}`;
        const cleanContact = phone.startsWith('+7') ? phone : `+7${phone.replace(/^\+/, '')}`;
        
        // Extract payment data if PDF content is available
        let paymentData = null;
        if (pdfContent && media.mimetype === 'application/pdf') {
            console.log('Extracting payment data from PDF...');
            paymentData = await extractPaymentData(pdfContent, media.filename);
        }
        
        // Create message for accounting group
        let forwardMessage = `💰 *Новый платежный документ*\n\n` +
                            `📞 *От:* ${cleanContact}\n` +
                            `📅 *Время:* ${new Date().toLocaleString('ru-RU')}\n`;
        
        // Add extracted payment data if available
        if (paymentData) {
            forwardMessage += `\n📊 *Данные платежа:*\n` +
                             `👤 *Отправитель:* ${paymentData.senderName}\n` +
                             `🏢 *Получатель:* ${paymentData.recipientName}\n` +
                             `💵 *Сумма:* ${paymentData.amount}\n`;
        }
        
        forwardMessage += `\nПожалуйста, обработайте платежный документ.`;
        
        // Send text message first
        await client.sendMessage(ACCOUNTING_GROUP_ID, forwardMessage);
        
        // Forward the actual file
        const mediaMessage = new MessageMedia(media.mimetype, media.data, media.filename);
        await client.sendMessage(ACCOUNTING_GROUP_ID, mediaMessage);
        
        console.log(`Payment file forwarded to accounting group from ${message.from}${paymentData ? ' with extracted payment data' : ''}`);
        
    } catch (error) {
        console.error('Error forwarding payment file to accounting:', error);
        await sendReplyWithTyping(message, "Документ получен, но возникла ошибка при передаче в бухгалтерию. Пожалуйста, обратитесь в офис.");
    }
}

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

        // Add current date/time context to the system prompt
        const dateTimeContext = getCurrentDateTimeContext();
        const enhancedSystemPrompt = `${SYSTEM_PROMPT}\n\n${CONFIRMATION_DELEGATION_RULES}\n\n${dateTimeContext}`;

        const completion = await openai.chat.completions.create({
            model: OPENAI_MODEL,
            messages: [{ role: "system", content: enhancedSystemPrompt }, ...openAIMessages],
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
    const history = await historyManager.getHistory(chatId);
    if (!history || history.length === 0) return;

    console.log(`Summarizing ${history.length} messages for chat ${chatId}...`);
    const summarizationMessages = [
        ...history,
        { role: "user", type: 'text', content: SUMMARIZATION_PROMPT }
    ];

    try {
        const summaryResponse = await getOpenAIResponse(summarizationMessages);
        const recentHistory = history.slice(-5);
        const newHistory = [
            { role: "system", type: 'text', content: `Summary of previous conversation: ${summaryResponse}` },
            ...recentHistory
        ];
        await historyManager.updateHistory(chatId, newHistory);
        console.log(`Summarization complete for ${chatId}.`);
    } catch (error) {
        console.error(`Failed to summarize history for ${chatId}:`, error);
    }
}

// --- GROUP ROUTING FUNCTIONS ---

async function analyzeRequestForRouting(history) {
    const ROUTING_PROMPT = `Analyze the conversation history and determine if this requires routing to a working group.

DO NOT route if the user is:
- Saying thank you, expressing gratitude (спасибо, благодарю, etc.)
- Asking simple clarifying questions (что?, зачем?, почему?)
- Having casual conversation
- Responding to bot confirmations
- Just acknowledging previous messages

ONLY route if there is a NEW, SPECIFIC request for:
1. GENERAL - Complaints, emergencies, repairs, maintenance issues, building problems, utilities, heating, water, electricity, elevators, cleaning, security, noise complaints
2. ACCOUNTING - Documentation requests, receipts, payment issues, account statements, billing questions, payment confirmations, salary and financial matters (BUT NOT account number lookup requests - those are handled automatically). Include phrases like: «расчет начислений», «расчёт за [месяц/период]», «детализация начислений», «подготовьте расчет/расчёт», «свод начислений», «справка по начислениям», «расчет за июнь-июль» — treat these as ACCOUNTING.
3. ADMIN - When the bot cannot help, gets stuck, requires human intervention, or when the user is frustrated with automated responses

DO NOT route account number/лицевой счет lookup requests - the bot handles these automatically with Excel data.

Return ONLY one word: GENERAL, ACCOUNTING, ADMIN, or NONE

If this is just a thank you, question, or casual response - return NONE.

Important: Do NOT propose confirmations or say anything about sending/forwarding a request; just classify.`;

    try {
        const completion = await openai.chat.completions.create({
            model: OPENAI_MODEL,
            messages: [
                { role: "system", content: ROUTING_PROMPT },
                ...history.map(m => ({role: m.role, content: m.content}))
            ],
            max_tokens: 10
        });
        const response = completion.choices[0].message.content.trim().toUpperCase();
        return ['GENERAL', 'ACCOUNTING', 'ADMIN', 'NONE'].includes(response) ? response : 'NONE';
    } catch (error) {
        console.error('Error analyzing request for routing:', error);
        return 'NONE'; // Default fallback - don't route on error
    }
}

async function analyzeRequestCompleteness(history, routingType) {
    const COMPLETENESS_PROMPT = `Analyze the conversation history to determine if there is enough information to create a complete ${routingType.toLowerCase()} request.

CRITICAL: For ALL request types, the user's FULL NAME (фамилия и имя) is MANDATORY and must be clearly stated in the conversation. Both surname and first name are required.

For a GENERAL request (complaints, repairs, emergencies), check if the following information is available:
1. FULL NAME (фамилия и имя) of the person making the request - REQUIRED, must include both surname and first name
2. Clear description of the problem/issue
3. Location details (apartment number, floor, specific area)
4. Any relevant context (when it started, frequency, etc.)

For an ACCOUNTING request (documents, receipts, financial), check if:
1. FULL NAME (фамилия и имя) of the person making the request - REQUIRED, must include both surname and first name
2. Basic document type mentioned (квитанция, справка, документ)
3. General timeframe if relevant (не обязательно точные даты)
For simple document requests like receipts - minimal information is sufficient.

For an ADMIN request, check if:
1. FULL NAME (фамилия и имя) of the person making the request - REQUIRED, must include both surname and first name
2. Clear description of the complex issue
3. Previous attempts to resolve
4. Specific assistance needed

IMPORTANT: If the full name is missing or incomplete (only first name, only surname, etc.), the request is NOT complete. Both surname and first name are required.

Return JSON with:
- "complete": true/false
- "missing_info": array of missing information types
- "clarifying_questions": array of specific questions to ask (max 2 questions)

If the full name is missing, always include a question asking for it specifically: "Укажите ваши фамилию и имя"`;

    try {
        const completion = await openai.chat.completions.create({
            model: OPENAI_MODEL,
            messages: [
                { role: "system", content: COMPLETENESS_PROMPT },
                ...history.map(m => ({role: m.role, content: m.content}))
            ],
            response_format: { type: "json_object" },
            max_tokens: 300
        });
        
        const analysis = JSON.parse(completion.choices[0].message.content);
        return {
            complete: analysis.complete || false,
            missingInfo: analysis.missing_info || [],
            clarifyingQuestions: analysis.clarifying_questions || []
        };
    } catch (error) {
        console.error('Error analyzing request completeness:', error);
        // Default to incomplete to be safe
        return {
            complete: false,
            missingInfo: ['Общая информация'],
            clarifyingQuestions: ['Можете предоставить более подробную информацию о вашем запросе?']
        };
    }
}

async function formatRequestForGroup(history, chatId, routingType) {
    // Check if we have cached resident data first
    const cachedData = residentDataCache[chatId];
    const cacheAge = cachedData ? (Date.now() - cachedData.timestamp) / (1000 * 60) : Infinity; // age in minutes
    const cacheValid = cachedData && cacheAge < 30; // Cache valid for 30 minutes
    
    const phone = `+${chatId.split('@')[0]}`;
    const cleanContact = phone.startsWith('+7') ? phone : `+7${phone.replace(/^\+/, '')}`;
    
    if (cacheValid) {
        console.log(`Using cached resident data for ${chatId} (age: ${Math.round(cacheAge)}min)`);
        
        // Extract only issue and details from history, use cached data for name/address
        const ISSUE_EXTRACTION_PROMPT = `Extract only the issue and details for a ${routingType.toLowerCase()} request from the conversation:

1. Issue: Brief description of the current request (one sentence)
2. Details: Specific information about this request (maximum 40 words)

Return JSON with keys: 'issue', 'details'
Focus ONLY on the most recent request, not previous topics.`;

        try {
            const completion = await openai.chat.completions.create({
                model: OPENAI_MODEL,
                messages: [
                    { role: "system", content: ISSUE_EXTRACTION_PROMPT },
                    ...history.map(m => ({role: m.role, content: m.content}))
                ],
                response_format: { type: "json_object" },
                max_tokens: 150
            });
            
            const extractedIssue = JSON.parse(completion.choices[0].message.content);
            
            return {
                fullName: cachedData.fullName,
                address: cachedData.address,
                contact: cleanContact,
                issue: extractedIssue.issue || 'Не указано',
                details: extractedIssue.details || 'Не указано'
            };
        } catch (error) {
            console.error('Error extracting issue from cached data:', error);
            return {
                fullName: cachedData.fullName,
                address: cachedData.address,
                contact: cleanContact,
                issue: 'Требуется обработка запроса',
                details: 'Ошибка при извлечении деталей запроса'
            };
        }
    }
    
    // Fallback to full extraction if no cached data
    const FORMATTING_PROMPT = `Analyze the conversation history and extract the following information for a ${routingType.toLowerCase()} request:

1. Full Name: Extract the complete name (surname and first name) of the person making the request
2. Address: Extract the full house address mentioned in the conversation
3. Contact: The phone number (should start with +7, without c.us)
4. Issue: Brief description of the reason for the request (one sentence)
5. Details: More information about the issue, but concise - maximum 40 words. Focus ONLY on the most recent relevant request, do not mix different requests.

Return the data in JSON format with keys: 'fullName', 'address', 'contact', 'issue', 'details'
If any information is missing, use 'Не указано' for that field.

Formatting rules:
- 'address' must be a clean postal address string only (e.g., "Магомеда Гаджиева 73а, кв. 92"). Do not include лишние пояснения в скобках, даты, чужие адреса, слова вроде "бывший".
- 'issue' should be one short sentence (no prefixes like "Заявка на" or "Просьба"), only the essence.
- 'details' should be concise (<=40 words) and refer only to the most recent request topic, without duplicating 'issue'.

Important: For details, analyze only the latest request topic and provide focused information without mixing different issues.`;

    try {
        const completion = await openai.chat.completions.create({
            model: OPENAI_MODEL,
            messages: [
                { role: "system", content: FORMATTING_PROMPT },
                ...history.map(m => ({role: m.role, content: m.content}))
            ],
            response_format: { type: "json_object" },
            max_tokens: 200
        });
        
        const extractedData = JSON.parse(completion.choices[0].message.content);
        
        // Ensure contact starts with +7 and clean it
        const phone = `+${chatId.split('@')[0]}`;
        const cleanContact = phone.startsWith('+7') ? phone : `+7${phone.replace(/^\+/, '')}`;
        
        return {
            fullName: extractedData.fullName || 'Не указано',
            address: extractedData.address || 'Не указано',
            contact: cleanContact,
            issue: extractedData.issue || 'Не указано',
            details: extractedData.details || 'Не указано'
        };
    } catch (error) {
        console.error('Error formatting request:', error);
        const phone = `+${chatId.split('@')[0]}`;
        const cleanContact = phone.startsWith('+7') ? phone : `+7${phone.replace(/^\+/, '')}`;
        
        return {
            fullName: 'Не указано',
            address: 'Не указано',
            contact: cleanContact,
            issue: 'Требуется обработка запроса',
            details: 'Ошибка при обработке деталей запроса'
        };
    }
}

async function sendRequestToGroup(groupId, requestData, routingType) {
    if (!groupId) {
        console.error(`${routingType} group ID is not configured`);
        return { success: false, requestId: null };
    }

    // Generate unique request ID
    const requestId = uuidv4();
    const shortRequestId = '#' + requestId.substring(0, 8).toUpperCase();

    const groupName = routingType === 'GENERAL' ? 'Общие вопросы' : 
                     routingType === 'ACCOUNTING' ? 'Бухгалтерия' : 'Администрация';
    
    // Get current timestamp
    const now = new Date();
    const timeString = now.toLocaleTimeString('ru-RU', {
        hour: '2-digit',
        minute: '2-digit'
    });
    const dateString = now.toLocaleDateString('ru-RU', {
        day: 'numeric',
        month: 'short',
        weekday: 'short'
    });
    const creationTime = `${timeString} (${dateString})`;
    
    const requestMessage = `🔔 *Новый запрос - ${groupName}*\n\n` +
                          `🆔 *Номер заявки:* ${shortRequestId}\n` +
                          ` *Время создания:* ${creationTime}\n\n` +
                          ` *ФИО:* ${requestData.fullName}\n` +
                          ` *Адрес:* ${requestData.address}\n` +
                          ` *Контакт:* ${requestData.contact}\n` +
                          ` *Проблема:* ${requestData.issue}\n` +
                          ` *Детали:* ${requestData.details}`;

    try {
        await client.sendMessage(groupId, requestMessage);
        console.log(`Request sent to ${routingType} group: ${groupId}, Request ID: ${shortRequestId}`);
        return { success: true, requestId: shortRequestId };
    } catch (error) {
        console.error(`Error sending request to ${routingType} group:`, error);
        return { success: false, requestId: null };
    }
}

// --- CONFIRMATION HANDLING FUNCTIONS ---

/**
 * Formats a confirmation message showing collected request data
 * @param {Object} requestData - The formatted request data
 * @param {string} routingType - The type of routing (GENERAL, ACCOUNTING, ADMIN)
 * @param {Array} history - The conversation history to extract full name
 * @returns {Promise<string>} The formatted confirmation message
 */
async function formatConfirmationMessage(requestData, routingType, history) {
    const typeNames = {
        'GENERAL': 'службу управления домом',
        'ACCOUNTING': 'бухгалтерию',
        'ADMIN': 'администрацию для подключения живого ассистента'
    };
    
    const typeName = typeNames[routingType] || 'службу поддержки';
    
    // Extract full name from conversation history (JSON for robustness)
    let fullName = 'Не указано';
    try {
        const extractionCompletion = await openai.chat.completions.create({
            model: OPENAI_MODEL,
            messages: [
                { role: "system", content: "Извлеки полное ФИО (как одна строка) из этой переписки. Верни строго JSON вида {\"fullName\": \"Иванов Иван Иванович\"}. Если ФИО не найдено — верни {\"fullName\": \"Не указано\"}. Никаких других полей и текста." },
                ...history.map(m => ({role: m.role, content: m.content}))
            ],
            response_format: { type: "json_object" },
            max_tokens: 40,
            temperature: 0.1
        });
        const extracted = JSON.parse(extractionCompletion.choices[0].message.content || '{}');
        if (extracted.fullName && typeof extracted.fullName === 'string' && extracted.fullName.trim().length > 2) {
            fullName = extracted.fullName.trim();
        }
    } catch (error) {
        console.error('Error extracting full name for confirmation:', error);
    }
    
    return `📋 *Проверьте данные перед отправкой в ${typeName}:*\n\n` +
           `👤 *ФИО:* ${fullName}\n` +
           `📍 *Адрес:* ${requestData.address}\n` +
           `❗ *Проблема:* ${requestData.issue}\n` +
           `📝 *Детали:* ${requestData.details}\n\n` +
           `❓ *Данные корректны?* Ответьте "да" или "нет".`;
}

/**
 * Uses AI to analyze if a message contains confirmation (yes/no) by understanding context and intent
 * @param {string} messageText - The user's message text
 * @param {Array} conversationHistory - Recent conversation history for context
 * @returns {Promise<string|null>} 'yes', 'no', or null if not a confirmation
 */
async function analyzeConfirmationResponse(messageText) {
    const CONFIRMATION_ANALYSIS_PROMPT = `Analyze if the user's response is confirming or denying the data shown to them.

Context: The bot just showed collected data to the user and asked "Данные корректны? Ответьте да или нет" (Are the data correct? Answer yes or no).

User's response: "${messageText}"

Determine if this is:
1. CONFIRMATION (yes) - User agrees the data is correct, wants to proceed
2. DENIAL (no) - User disagrees with the data, wants to make changes  
3. NOT_A_CONFIRMATION (null) - User is asking something else, changing topic, or being ambiguous

IMPORTANT: Compound positive responses should be "yes":
- "да, верно" → yes (not no!)
- "да, правильно" → yes
- "да, хорошо" → yes
- "да, все так" → yes

ONLY mixed responses with contradictions should be "no":
- "да, но адрес неправильный" → no (contradiction)
- "да, только имя не то" → no (exception/problem)

Consider:
- Intent behind the message, not just keywords
- Context of data verification
- Natural language variations in Russian and English
- Pure positive compounds (да + positive word) = yes
- Mixed responses with "но/только/except/but" = no
- Questions or off-topic responses should be "null"

Examples:
- "да" → yes
- "да, верно" → yes
- "да, правильно" → yes  
- "yes, correct" → yes  
- "все правильно" → yes
- "нет" → no
- "да, но адрес неправильный" → no
- "no, the address is wrong" → no
- "не согласен с адресом" → no
- "А когда будет готово?" → null
- "Спасибо" → null
- "может быть" → null

Respond with exactly one word: "yes", "no", or "null"`;

    try {
        const completion = await openai.chat.completions.create({
            model: OPENAI_MODEL,
            messages: [
                { role: "system", content: CONFIRMATION_ANALYSIS_PROMPT },
                { role: "user", content: messageText }
            ],
            max_tokens: 10,
            temperature: 0.1 // Low temperature for consistent responses
        });

        const response = completion.choices[0].message.content.trim().toLowerCase();
        
        if (response === 'yes') return 'yes';
        if (response === 'no') return 'no';
        if (response === 'null') return null;
        
        // Fallback: if AI returns unexpected response, default to null (not a confirmation)
        console.warn(`Unexpected AI confirmation analysis response: ${response}. Defaulting to null.`);
        return null;
        
    } catch (error) {
        console.error('Error analyzing confirmation response:', error);
        // Fallback to basic keyword detection if AI fails
        return basicConfirmationFallback(messageText);
    }
}

/**
 * Fallback confirmation detection using basic patterns (used if AI fails)
 * @param {string} messageText - The user's message text
 * @returns {string|null} 'yes', 'no', or null
 */
function basicConfirmationFallback(messageText) {
    const text = messageText.toLowerCase().trim();
    
    // Very basic patterns as fallback
    if (['да', 'yes', 'ок', 'ok'].includes(text)) {
        return 'yes';
    }
    
    if (['нет', 'no'].includes(text)) {
        return 'no';
    }
    
    return null;
}

/**
 * Detects if user has changed topics or is ignoring confirmation request
 * @param {string} messageText - The user's message text
 * @returns {Promise<boolean>} True if topic change detected
 */
async function detectTopicChange(messageText) {
    const TOPIC_CHANGE_PROMPT = `The bot recently asked the user to confirm request data with "Данные корректны? Ответьте да или нет" (Are the data correct? Answer yes or no).

User's response: "${messageText}"

Determine if this is a TOPIC CHANGE where the user:
1. Started a completely new request/complaint unrelated to the pending confirmation
2. Asked a different question ignoring the confirmation
3. Said something dismissive like "не буду ничего проверять", "надоели вы уже", "отвали"
4. Made a new unrelated complaint or request

Return "true" if this is clearly a topic change that should cancel the pending confirmation.
Return "false" if this is still related to the confirmation request (corrections, clarifications, etc.)

Examples:
- "не буду ничего проверять" → true (dismissive)
- "надоели вы уже" → true (dismissive) 
- "каждый раз говорите заявку передадим" → true (dismissive)
- "и ничего не делаете!" → true (dismissive)
- "У меня другая проблема с лифтом" → true (new request)
- "Когда починят отопление?" → true (new question)
- "я не хочу уже создавать заявку!" → true (dismissive)
- "отвали!" → true (dismissive)
- "Адрес неправильный" → false (correction)
- "Не согласен с адресом" → false (clarification)
- "Можете исправить имя?" → false (correction)

Respond with exactly one word: "true" or "false"`;

    try {
        const completion = await openai.chat.completions.create({
            model: OPENAI_MODEL,
            messages: [
                { role: "system", content: TOPIC_CHANGE_PROMPT },
                { role: "user", content: messageText }
            ],
            max_tokens: 5,
            temperature: 0.1
        });

        const response = completion.choices[0].message.content.trim().toLowerCase();
        return response === 'true';
        
    } catch (error) {
        console.error('Error detecting topic change:', error);
        // Fallback: detect basic dismissive patterns
        const dismissivePatterns = [
            'надоел', 'отвал', 'не буду', 'не хочу', 'каждый раз', 'ничего не дела'
        ];
        const text = messageText.toLowerCase();
        return dismissivePatterns.some(pattern => text.includes(pattern));
    }
}

/**
 * Processes the user's confirmation response
 * @param {string} chatId - The chat ID
 * @param {string} confirmation - 'yes' or 'no'
 * @param {Object} message - The original WhatsApp message
 */
async function processConfirmationResponse(chatId, confirmation, message) {
    const pendingRequest = pendingRequests[chatId];
    if (!pendingRequest) {
        return false;
    }
    
    if (confirmation === 'yes') {
        // User confirmed - proceed with sending the request
        const { requestData, routingType, history } = pendingRequest;
        
        let groupId;
        switch (routingType) {
            case 'GENERAL':
                groupId = GENERAL_GROUP_ID;
                break;
            case 'ACCOUNTING':
                groupId = ACCOUNTING_GROUP_ID;
                break;
            case 'ADMIN':
                groupId = ADMIN_GROUP_ID;
                break;
        }
        
        const requestResult = await sendRequestToGroup(groupId, requestData, routingType);
        
        if (requestResult.success) {
            let successMessage;
            const workingHours = isWorkingHours();
            const contactTime = workingHours ? 'в ближайшее время' : 'в рабочее время';
            
            if (routingType === 'GENERAL') {
                successMessage = `✅ Ваш запрос передан в службу управления домом.\n\n🆔 *Номер вашей заявки: ${requestResult.requestId}*\n\nПри необходимости - специалист свяжется с вами ${contactTime}.`;
            } else if (routingType === 'ACCOUNTING') {
                successMessage = `✅ Ваш запрос передан в бухгалтерию.\n\n🆔 *Номер вашей заявки: ${requestResult.requestId}*\n\nСпециалист свяжется с вами ${contactTime}.`;
            } else if (routingType === 'ADMIN') {
                successMessage = `✅ Ваш запрос передан для подключения живого ассистента.\n\n🆔 *Номер вашей заявки: ${requestResult.requestId}*\n\nС вами свяжутся ${contactTime}.`;
                // For admin routing, disable AI responses for this user
                await muteChat(chatId, 24 * 60 * 60 * 1000); // 24 hours in milliseconds
            }
            
            history.push({ role: "assistant", type: 'text', content: successMessage });
            await historyManager.updateHistory(chatId, history);
            await sendReplyWithTyping(message, successMessage);
        } else {
            const errorMessage = 'Произошла ошибка при отправке заявки. Пожалуйста, обратитесь в офис по номеру +7 (800) 444-52-05.';
            history.push({ role: "assistant", type: 'text', content: errorMessage });
            await historyManager.updateHistory(chatId, history);
            await sendReplyWithTyping(message, errorMessage);
        }
    } else {
        // User declined - ask what needs to be corrected
        const correctionMessage = 'Хорошо, расскажите что нужно исправить или дополнить в вашем запросе.';
        const history = await historyManager.getHistory(chatId);
        history.push({ role: "assistant", type: 'text', content: correctionMessage });
        await historyManager.updateHistory(chatId, history);
        await sendReplyWithTyping(message, correctionMessage);
    }
    
    // Clean up the pending request
    delete pendingRequests[chatId];
    return true;
}

// Clean up expired pending requests and cached data every 5 minutes
setInterval(() => {
    const now = Date.now();
    const expireTime = 10 * 60 * 1000; // 10 minutes
    const cacheExpireTime = 60 * 60 * 1000; // 1 hour for cached data
    
    for (const [chatId, request] of Object.entries(pendingRequests)) {
        if (now - request.timestamp > expireTime) {
            delete pendingRequests[chatId];
            console.log(`Expired pending request for ${chatId}`);
        }
    }
    
    // Clean up expired cached resident data
    for (const [chatId, cachedData] of Object.entries(residentDataCache)) {
        if (now - cachedData.timestamp > cacheExpireTime) {
            delete residentDataCache[chatId];
            console.log(`Expired cached resident data for ${chatId}`);
        }
    }
}, 5 * 60 * 1000);

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

        if (fullName && address) {
            console.log(`Looking up account for: ${fullName} at ${address}`);
            const accountInfo = excelParser.findResidentAccount(fullName, address);
            
            if (accountInfo) {
                // Cache the resident data for future use
                residentDataCache[chatId] = {
                    fullName: fullName,
                    address: address,
                    accountInfo: accountInfo,
                    timestamp: Date.now()
                };
                
                const accountMessage = `🏠 Найден ваш лицевой счет:\n\n📋 Номер: ${accountInfo.accountNumber}\n👤 ФИО: ${accountInfo.fullName}\n🏠 Квартира: ${accountInfo.apartmentNumber}\n📍 Адрес: ${accountInfo.address}\n\nИспользуйте этот номер для входа в приложение УК "Прогресс".`;
                await sendMessageWithTyping(chatId, accountMessage);
                // Add the account info to conversation history
                history.push({ role: "assistant", type: 'text', content: accountMessage });
                console.log(`Account info sent to ${chatId}: ${accountInfo.accountNumber}. Data cached.`);
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
