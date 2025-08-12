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
const SERVICE_REQUESTS_FILE_PATH = './service-requests.json';
// Dedup considers only requests within this time window
const SERVICE_REQUEST_DUP_WINDOW_HOURS = Number(process.env.SERVICE_REQUEST_DUP_WINDOW_HOURS || 24);
const SERVICE_REQUEST_DUP_WINDOW_MS = SERVICE_REQUEST_DUP_WINDOW_HOURS * 60 * 60 * 1000;

// --- WORK GROUP INTEGRATION ---
const WORK_GROUP_ID = process.env.WORK_GROUP_ID || null;
const ACCOUNTING_GROUP_ID = process.env.ACCOUNTING_GROUP_ID || null;
const REQUEST_CONFIRMATION_PROMPT = "Read the following message. Does it confirm that a service request has been successfully created and all necessary information (like address and time) has been collected? Answer only with 'yes' or 'no'.";
const REQUEST_EXTRACTION_PROMPT = "Extract the user's address and a description of the issue from the conversation. Return the data in JSON format with the keys: 'address', and 'issue'. If any information is missing, use the value 'null'.";
const ISSUE_SUMMARY_PROMPT = "Summarize the following issue in a two-three words  **in Russian**.";
const DETAILED_ISSUE_PROMPT = "Based on the LAST user request, generate a concise description of the user's issue **in Russian**, under 50 words.";
const ACCOUNT_EXTRACTION_PROMPT = "Analyze the ENTIRE conversation history and extract the full name and complete address for the person whose account is being requested. This could be the user themselves or someone they're asking about (like a family member). Information may be provided across multiple messages. Look for: 1) Full name (first name, last name) - may be provided in parts across different messages 2) Complete address including street name, house number, and apartment number - may also be provided in parts. Combine all address parts into a single address string. Return the data in JSON format with the keys: 'fullName' and 'address'. If any information is missing, use the value 'null'. Examples: fullName: 'Адакова Валерия Аликовна', address: 'Магомеда Гаджиева 73а, кв. 92'. Pay special attention to: - Names that may be provided as 'адакова валерия' first, then 'Адакова Валерия Аликовна' later - Addresses like 'магомед гаджиева 73а, 92кв' or 'магомед гаджиева 73а' + '92кв' separately";
const ACCOUNTING_DETECTION_PROMPT = "Analyze the following message and determine if it requires accounting department intervention. Answer 'yes' if the message contains: 1) Questions about specific debt amounts, balances, or payment details 2) Requests for documents (квитанция, справка, документы) 3) Disputes about charges or payments 4) Questions about calculations, recalculations, or payment history 5) Requests for account verification or balance checks 6) Complaints about incorrect billing. Answer 'yes' for messages asking about: долг, задолженность, баланс, сколько должен, переплата, расчет, перерасчет, оплата, счет, лицевой счет details. Answer only with 'yes' or 'no'.";

// --- ADMIN INTEGRATION ---
const ADMIN_GROUP_ID = process.env.ADMIN_GROUP_ID || null;
const ADMIN_STATE_FILE_PATH = './admin-state.json';

const SYSTEM_PROMPT = `Ты - Кристина, администратор УК "Прогресс".

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
let mutedChats = {}; // { [chatId]: { until: number | null } }
let excelParser = null;
let serviceRequestsState = {}; // { [chatId]: Array<{ addressNorm: string, issueNorm: string, addressRaw: string, issueSummaryRaw: string, createdAt: number }> }

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

async function sendReplyWithTyping(message, text) {
    try {
        const chat = await message.getChat();
        if (chat && !chat.isGroup) {
            const delayMs = calculateTypingDurationMs(text);
            await showTypingForDuration(chat, delayMs);
        }
        await message.reply(text);
    } catch (e) {
        try { await message.reply(text); } catch (_) {}
    }
}

async function sendMessageWithTyping(chatId, text) {
    try {
        const chat = await client.getChatById(chatId);
        if (chat && !chat.isGroup) {
            const delayMs = calculateTypingDurationMs(text);
            await showTypingForDuration(chat, delayMs);
        }
        await client.sendMessage(chatId, text);
    } catch (e) {
        try { await client.sendMessage(chatId, text); } catch (_) {}
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

// --- SERVICE REQUESTS DEDUP STATE ---
async function saveServiceRequestsState() {
    try {
        const data = JSON.stringify(serviceRequestsState, null, 2);
        await fs.writeFile(SERVICE_REQUESTS_FILE_PATH, data, 'utf8');
    } catch (e) {
        console.error('Error saving service requests state:', e);
    }
}

async function loadServiceRequestsState() {
    try {
        await fs.access(SERVICE_REQUESTS_FILE_PATH);
        const data = await fs.readFile(SERVICE_REQUESTS_FILE_PATH, 'utf8');
        serviceRequestsState = JSON.parse(data);
        console.log('Successfully loaded service requests state.');
    } catch (e) {
        if (e.code === 'ENOENT') {
            console.log('No service requests state file found. Starting with empty state.');
            serviceRequestsState = {};
        } else {
            console.error('Error loading service requests state:', e);
        }
    }
}

function normalizeText(text) {
    return String(text || '')
        .toLowerCase()
        .replace(/[\s\n\r\t]+/g, ' ')
        .trim();
}

function normalizeAddress(address) {
    return normalizeText(address).replace(/[.,;:]/g, '');
}

function findDuplicateServiceRequest(chatId, address, issueSummary) {
    const addressNorm = normalizeAddress(address);
    const issueNorm = normalizeText(issueSummary);
    const now = Date.now();
    const cutoff = now - SERVICE_REQUEST_DUP_WINDOW_MS;
    const entries = (serviceRequestsState[chatId] || []).filter(e => (e.createdAt || 0) >= cutoff);
    return entries.find(e => e.addressNorm === addressNorm && e.issueNorm === issueNorm) || null;
}

function recordServiceRequest(chatId, address, issueSummary) {
    const entry = {
        addressNorm: normalizeAddress(address),
        issueNorm: normalizeText(issueSummary),
        addressRaw: address,
        issueSummaryRaw: issueSummary,
        createdAt: Date.now()
    };
    if (!serviceRequestsState[chatId]) serviceRequestsState[chatId] = [];
    serviceRequestsState[chatId].push(entry);
    saveServiceRequestsState();
    return entry;
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

async function isDuplicateRequestAI(chatId, address, issueSummary, issueFullText) {
    try {
        const cutoff = Date.now() - SERVICE_REQUEST_DUP_WINDOW_MS;
        const previous = (serviceRequestsState[chatId] || []).filter(r => (r.createdAt || 0) >= cutoff).map((r, idx) => ({
            index: idx,
            address: r.addressRaw,
            issue: r.issueSummaryRaw,
            createdAt: r.createdAt
        }));

        // If nothing to compare, it's not a duplicate
        if (previous.length === 0) return false;

        const prompt = `Определи, является ли новая жалоба дубликатом одной из уже созданных заявок для этого жильца.
Считай дубликатом, если адрес совпадает (допускай незначительные различия в записи) и описывается та же проблема, даже если формулировка отличается.
Не считай дубликатом, если это явно новый отдельный случай или другая локация/узел в квартире/доме.

Текущая жалоба:
- Адрес: ${address}
- Кратко: ${issueSummary}
- Текст: ${issueFullText || ''}

Созданные ранее заявки (по этому чату):
${JSON.stringify(previous, null, 2)}

Ответь строго JSON с ключами: { "duplicate": true|false }.`;

        const completion = await openai.chat.completions.create({
            model: OPENAI_MODEL,
            messages: [{ role: 'user', content: prompt }],
            response_format: { type: 'json_object' },
            max_tokens: 50
        });
        const parsed = JSON.parse(completion.choices[0].message.content || '{}');
        return !!parsed.duplicate;
    } catch (e) {
        console.error('AI duplicate detection failed, falling back to non-duplicate:', e);
        return false;
    }
}

async function doesUserInsistNewRequestAI(history) {
    try {
        const recentUserTexts = getRecentUserMessages(history, 6);
        const prompt = `Проанализируй последние сообщения жильца и ответь, просит ли он ЯВНО оформить ОТДЕЛЬНУЮ новую заявку, а не просто уточняет статус/подробности.
Сообщения:
${JSON.stringify(recentUserTexts, null, 2)}

Ответь строго JSON: { "insist": true|false }.`;
        const completion = await openai.chat.completions.create({
            model: OPENAI_MODEL,
            messages: [{ role: 'user', content: prompt }],
            response_format: { type: 'json_object' },
            max_tokens: 30
        });
        const parsed = JSON.parse(completion.choices[0].message.content || '{}');
        return !!parsed.insist;
    } catch (e) {
        console.error('AI insist detection failed, defaulting to not insisting:', e);
        return false;
    }
}

async function generateNaturalDuplicateReplyAI(history, address, issueSummary) {
    try {
        const recentUserTexts = getRecentUserMessages(history, 6);
        const prompt = `Кратко и по-делу ответь жильцу на русском.
Контекст: по этой проблеме уже есть активная заявка. Адрес: ${address}. Кратко о проблеме: ${issueSummary}.
Задача ответа:
- Сообщи естественно, что заявка уже в работе.
- Предложи добавить новые детали, если появились (фото, время, уточнения).
- Не используй штампованные формулировки.
- Будь краткой.

Недавние сообщения жильца:
${JSON.stringify(recentUserTexts, null, 2)}
`;
        const completion = await openai.chat.completions.create({
            model: OPENAI_MODEL,
            messages: [{ role: 'user', content: prompt }],
            max_tokens: 120
        });
        const text = (completion.choices[0].message.content || '').trim();
        return text || 'Заявка уже в работе. Если появились новые детали, пришлите — обновлю. Нужна отдельная заявка — скажите.';
    } catch (e) {
        console.error('AI duplicate reply generation failed, using fallback:', e);
        return 'Заявка уже в работе. Если появились новые детали, пришлите — обновлю. Нужна отдельная заявка — скажите.';
    }
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
        // Check if any of the messages are accounting-related
         // Only check after we have some conversation context (at least 2 user messages or explicit request)
         const userMessages = history.filter(msg => msg.role === 'user');
         const lastUserMessage = messages[messages.length - 1].userHistoryEntry;
         const messageContent = typeof lastUserMessage.content === 'string' 
             ? lastUserMessage.content 
             : lastUserMessage.content[0]?.text || '';
             
         // Check if this is an accounting request using AI analysis only
         const hasEnoughContext = userMessages.length >= 2;
         
         if (hasEnoughContext && await isAccountingRequest(messageContent)) {
             console.log(`Accounting request detected from ${chatId}`);
             await handleAccountingRequest(chatId, history);
             
             // Let AI generate a natural confirmation response
             history.push({ role: "system", type: 'text', content: "The user's accounting request has been successfully forwarded to the accounting department. Provide a natural, helpful confirmation message in Russian." });
             
             const aiResponse = await getOpenAIResponse(history);
             
             // Remove the system message
             history.pop();
             
             // Add AI response to history and reply to user
             history.push({ role: "assistant", type: 'text', content: aiResponse });
             const lastMessage = messages[messages.length - 1].originalMessage;
             await sendReplyWithTyping(lastMessage, aiResponse);
             
             // Update conversation history and save
             conversationHistories[chatId] = history;
             await saveHistory();
             
             // Clear the buffer
             messageBuffers[chatId] = [];
             return;
         }
        
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
            // If this assistant message is a request confirmation, preflight duplicate logic
            const isConfirm = await isRequestCreationConfirmation(aiResponse);
            if (isConfirm) {
                const decision = await preflightServiceRequestDecision(chatId, history);
                const replyText = decision.allowCreate ? aiResponse : decision.replyText;
                history.push({ role: "assistant", type: 'text', content: replyText });
                conversationHistories[chatId] = history;
                const lastMessage = messages[messages.length - 1].originalMessage;
                await sendReplyWithTyping(lastMessage, replyText);
                await saveHistory();
                if (decision.allowCreate) {
                    await handleServiceRequest(chatId, history);
                }
            } else {
                // Normal reply
                history.push({ role: "assistant", type: 'text', content: aiResponse });
                conversationHistories[chatId] = history;
                const lastMessage = messages[messages.length - 1].originalMessage;
                await sendReplyWithTyping(lastMessage, aiResponse);
                await saveHistory();
            }
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

// Also process admin commands the bot sends itself in the admin group
client.on('message_create', async (message) => {
    try {
        if (!ADMIN_GROUP_ID) return;
        if (!message.fromMe) return;
        const targetChatId = message.to || message.from;
        const body = (message.body || '').trim();
        if (targetChatId === ADMIN_GROUP_ID && body.startsWith('!')) {
            await handleAdminCommand(message);
        }
    } catch (e) {
        console.error('Admin self-command error:', e);
    }
});

client.on('message', async message => {
    if (message.isStatus) return;

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
    await loadServiceRequestsState();
    
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

async function isAccountingRequest(messageContent) {
    try {
        const completion = await openai.chat.completions.create({
            model: OPENAI_MODEL,
            messages: [{
                role: "user",
                content: `${ACCOUNTING_DETECTION_PROMPT}\n\n${messageContent}`
            }],
            max_tokens: 10
        });
        const response = completion.choices[0].message.content.trim().toLowerCase();
        return response.includes('yes');
    } catch (error) {
        console.error("Error detecting accounting request:", error);
        return false;
    }
}

async function handleAccountingRequest(chatId, history) {
    if (!ACCOUNTING_GROUP_ID) {
        console.error("ACCOUNTING_GROUP_ID is not set in the .env file. Cannot send accounting request.");
        return;
    }

    try {
        const phone = `+${chatId.split('@')[0]}`;
        
        // Get conversation context for better understanding
        const conversationText = history
            .filter(msg => msg.role === 'user')
            .map(msg => typeof msg.content === 'string' ? msg.content : msg.content[0]?.text || '')
            .join(' ');
        
        // Use AI to extract reason and details
        const extractionPrompt = `Analyze this accounting request and extract: 1) Brief reason (2-3 words) 2) Detailed information. Text: "${conversationText}". Return JSON with keys: "reason", "details" in russian.`;
        
        let reason = 'Бухгалтерский запрос';
        let details = conversationText;
        
        try {
            const completion = await openai.chat.completions.create({
                model: OPENAI_MODEL,
                messages: [{ role: "user", content: extractionPrompt }],
                max_tokens: 200
            });
            
            const extracted = JSON.parse(completion.choices[0].message.content);
            reason = extracted.reason || reason;
            details = extracted.details || details;
        } catch (e) {
            console.log('Failed to extract structured info, using fallback');
        }
        
        const accountingMessage = `💰 Запрос в бухгалтерию\n\n📞 Телефон: ${phone}\n🏷️ Причина: ${reason}\n📝 Подробности: ${details}`;
        await client.sendMessage(ACCOUNTING_GROUP_ID, accountingMessage);
        console.log(`Accounting request from ${chatId} sent to accounting group.`);

        // Forward any media files from the conversation
        for (const msg of history) {
            if (msg.role === 'user' && msg.media && !msg.forwarded) {
                try {
                    const media = new MessageMedia(msg.media.mimetype, msg.media.data);
                    let caption = 'Приложенный файл от жильца.';
                    if (msg.type === 'image') {
                        caption = 'Изображение от жильца.';
                    } else if (msg.type === 'audio') {
                        caption = `Голосовое сообщение от жильца. Расшифровка: "${msg.content}"`;
                    } else if (msg.type === 'file') {
                        caption = `Документ от жильца: ${msg.media.filename || 'файл'}`;
                    }
                    await client.sendMessage(ACCOUNTING_GROUP_ID, media, { caption });
                    console.log(`Forwarded ${msg.type} from ${chatId} to accounting group.`);
                    msg.forwarded = true;
                } catch (e) {
                    console.error(`Failed to forward media from ${chatId} to accounting group:`, e);
                }
            }
        }

    } catch (error) {
        console.error(`Error handling accounting request for ${chatId}:`, error);
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

async function preflightServiceRequestDecision(chatId, history) {
    // Use the most recent extracted data to decide duplicate before we send acceptance
    try {
        const extractionCompletion = await openai.chat.completions.create({
            model: OPENAI_MODEL,
            messages: [...history.map(m => ({ role: m.role, content: m.content })), { role: 'user', content: REQUEST_EXTRACTION_PROMPT }],
            response_format: { type: 'json_object' }
        });
        const extracted = JSON.parse(extractionCompletion.choices[0].message.content || '{}');
        const { address, issue } = extracted;
        if (!address || !issue) {
            return { allowCreate: false, replyText: 'Нужны адрес и краткое описание проблемы, чтобы оформить заявку.', isDuplicate: false, insist: false };
        }

        const summaryCompletion = await openai.chat.completions.create({
            model: OPENAI_MODEL,
            messages: [{ role: 'user', content: `${ISSUE_SUMMARY_PROMPT}: ${issue}` }],
            max_tokens: 10
        });
        const issueSummary = (summaryCompletion.choices[0].message.content || '').trim();

        const duplicateRecord = findDuplicateServiceRequest(chatId, address, issueSummary);
        let isDuplicate = false;
        if (duplicateRecord) {
            isDuplicate = true;
        } else {
            isDuplicate = await isDuplicateRequestAI(chatId, address, issueSummary, issue);
        }

        if (!isDuplicate) return { allowCreate: true, isDuplicate: false, insist: false };

        const insist = await doesUserInsistNewRequestAI(history);
        if (insist) return { allowCreate: true, isDuplicate: true, insist };

        const naturalReply = await generateNaturalDuplicateReplyAI(history, address, issueSummary);
        return { allowCreate: false, replyText: naturalReply, isDuplicate: true, insist: false };

    } catch (e) {
        console.error('preflightServiceRequestDecision failed, defaulting to allow:', e);
        return { allowCreate: true, isDuplicate: false, insist: false };
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
            // Summarize issue first to use in deduplication key
            const summaryCompletion = await openai.chat.completions.create({
                model: OPENAI_MODEL,
                messages: [{ role: "user", content: `${ISSUE_SUMMARY_PROMPT}: ${issue}` }],
                max_tokens: 10
            });
            const issueSummary = summaryCompletion.choices[0].message.content.trim();

            // Duplicate prevention with AI reasoning
            const duplicateRecord = findDuplicateServiceRequest(chatId, address, issueSummary);
            let isDuplicate = false;
            let insistNew = false;
            if (duplicateRecord) {
                isDuplicate = true;
            } else {
                // Ask AI if this appears to be a duplicate of previous requests for this chat
                isDuplicate = await isDuplicateRequestAI(chatId, address, issueSummary, issue);
            }

            if (isDuplicate) {
                // Ask AI if the user insists on a separate new request
                insistNew = await doesUserInsistNewRequestAI(history);
                if (!insistNew) {
                    const naturalReply = await generateNaturalDuplicateReplyAI(history, address, issueSummary);
                    await sendMessageWithTyping(chatId, naturalReply);
                    history.push({ role: 'assistant', type: 'text', content: naturalReply });
                    conversationHistories[chatId] = history;
                    await saveHistory();
                    return;
                }
            }

            const detailedIssueCompletion = await openai.chat.completions.create({
                model: OPENAI_MODEL,
                messages: [...history.map(m => ({role: m.role, content: m.content})), { role: "user", content: DETAILED_ISSUE_PROMPT }],
                max_tokens: 150
            });
            const detailedIssue = detailedIssueCompletion.choices[0].message.content.trim();

            // Avoid sending two assistant messages back-to-back: if we got here via preflight (isDuplicate and not insistNew) we would have returned earlier.
            const header = (isDuplicate && insistNew) ? '🆕 Повторная заявка от жильца' : '🆕 Новая заявка от жильца';
            const requestMessage = `${header}\n\n📞 Телефон: ${phone}\n📍 Адрес: ${address}\n❗️ Проблема: ${issueSummary}\n\n📝 Описание:\n${detailedIssue}`;
            await client.sendMessage(WORK_GROUP_ID, requestMessage);
            console.log(`Service request text from ${chatId} sent to work group.`);

            // Record to dedup state
            recordServiceRequest(chatId, address, issueSummary);

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