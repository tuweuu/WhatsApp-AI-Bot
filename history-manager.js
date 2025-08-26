const fs = require('fs').promises;
const path = require('path');

/**
 * New file-based history management system
 * Each chat gets its own history file in the histories/ directory
 */
class HistoryManager {
    constructor(baseDir = './histories') {
        this.baseDir = baseDir;
        this.conversationHistories = {}; // In-memory cache
        this.maxHistoryLength = 50;
    }

    /**
     * Initialize the history manager
     */
    async initialize() {
        try {
            await fs.mkdir(this.baseDir, { recursive: true });
            console.log(`History manager initialized with directory: ${this.baseDir}`);
        } catch (error) {
            console.error('Error initializing history manager:', error);
        }
    }

    /**
     * Get the file path for a specific chat
     */
    getChatFilePath(chatId) {
        // Sanitize chat ID for filename (replace @ and . with _)
        const sanitizedId = chatId.replace(/[@.]/g, '_');
        return path.join(this.baseDir, `${sanitizedId}.json`);
    }

    /**
     * Load history for a specific chat
     */
    async loadChatHistory(chatId) {
        try {
            const filePath = this.getChatFilePath(chatId);
            await fs.access(filePath);
            const data = await fs.readFile(filePath, 'utf8');
            const history = JSON.parse(data);
            this.conversationHistories[chatId] = history;
            console.log(`Loaded history for chat ${chatId} (${history.length} messages)`);
            return history;
        } catch (error) {
            if (error.code === 'ENOENT') {
                // File doesn't exist, start with empty history
                this.conversationHistories[chatId] = [];
                return [];
            } else {
                console.error(`Error loading history for chat ${chatId}:`, error);
                this.conversationHistories[chatId] = [];
                return [];
            }
        }
    }

    /**
     * Save history for a specific chat
     */
    async saveChatHistory(chatId) {
        try {
            const history = this.conversationHistories[chatId] || [];
            if (history.length === 0) {
                // Don't create files for empty histories
                return;
            }

            const filePath = this.getChatFilePath(chatId);
            const data = JSON.stringify(history, null, 2);
            await fs.writeFile(filePath, data, 'utf8');
            console.log(`Saved history for chat ${chatId} (${history.length} messages)`);
        } catch (error) {
            console.error(`Error saving history for chat ${chatId}:`, error);
        }
    }

    /**
     * Get history for a chat (loads from file if not in memory)
     */
    async getHistory(chatId) {
        if (!this.conversationHistories[chatId]) {
            await this.loadChatHistory(chatId);
        }
        return this.conversationHistories[chatId] || [];
    }

    /**
     * Add a message to chat history
     */
    async addMessage(chatId, message) {
        if (!this.conversationHistories[chatId]) {
            await this.loadChatHistory(chatId);
        }
        
        this.conversationHistories[chatId].push(message);
        
        // Auto-save after adding message
        await this.saveChatHistory(chatId);
        
        // Check if summarization is needed
        if (this.conversationHistories[chatId].length > this.maxHistoryLength) {
            console.log(`History for ${chatId} exceeds limit. Triggering summarization.`);
            return true; // Indicates summarization needed
        }
        
        return false;
    }

    /**
     * Update the entire history for a chat (used after summarization)
     */
    async updateHistory(chatId, newHistory) {
        this.conversationHistories[chatId] = newHistory;
        await this.saveChatHistory(chatId);
    }

    /**
     * Delete history for a chat (used for reset command)
     */
    async deleteHistory(chatId) {
        try {
            const filePath = this.getChatFilePath(chatId);
            await fs.unlink(filePath);
            delete this.conversationHistories[chatId];
            console.log(`Deleted history for chat ${chatId}`);
        } catch (error) {
            if (error.code !== 'ENOENT') {
                console.error(`Error deleting history for chat ${chatId}:`, error);
            }
            // Always remove from memory even if file deletion fails
            delete this.conversationHistories[chatId];
        }
    }

    /**
     * Get list of all chat IDs that have history files
     */
    async getAllChatIds() {
        try {
            const files = await fs.readdir(this.baseDir);
            return files
                .filter(file => file.endsWith('.json'))
                .map(file => {
                    // Convert filename back to chat ID
                    const sanitizedId = file.replace('.json', '');
                    return sanitizedId.replace(/_/g, '@').replace('@c_us', '@c.us');
                });
        } catch (error) {
            console.error('Error getting chat IDs:', error);
            return [];
        }
    }

    /**
     * Get statistics about the history storage
     */
    async getStats() {
        try {
            const chatIds = await this.getAllChatIds();
            const stats = {
                totalChats: chatIds.length,
                chatsInMemory: Object.keys(this.conversationHistories).length,
                totalMessages: 0,
                averageMessagesPerChat: 0
            };

            // Count total messages
            for (const chatId of chatIds) {
                const history = await this.getHistory(chatId);
                stats.totalMessages += history.length;
            }

            stats.averageMessagesPerChat = stats.totalChats > 0 
                ? Math.round(stats.totalMessages / stats.totalChats) 
                : 0;

            return stats;
        } catch (error) {
            console.error('Error getting stats:', error);
            return { totalChats: 0, chatsInMemory: 0, totalMessages: 0, averageMessagesPerChat: 0 };
        }
    }

    /**
     * Migrate from old history.json format to new file-based system
     */
    async migrateFromOldFormat(oldHistoryPath = './history.json') {
        try {
            console.log('Starting migration from old history format...');
            
            // Check if old history file exists
            await fs.access(oldHistoryPath);
            
            // Read old history
            const data = await fs.readFile(oldHistoryPath, 'utf8');
            const oldHistories = JSON.parse(data);
            
            let migratedCount = 0;
            
            // Migrate each chat
            for (const [chatId, history] of Object.entries(oldHistories)) {
                if (Array.isArray(history) && history.length > 0) {
                    this.conversationHistories[chatId] = history;
                    await this.saveChatHistory(chatId);
                    migratedCount++;
                }
            }
            
            console.log(`Migration completed: ${migratedCount} chats migrated`);
            
            // Backup old file
            const backupPath = `${oldHistoryPath}.backup.${Date.now()}`;
            await fs.rename(oldHistoryPath, backupPath);
            console.log(`Old history file backed up to: ${backupPath}`);
            
            return { success: true, migratedCount, backupPath };
        } catch (error) {
            if (error.code === 'ENOENT') {
                console.log('No old history file found, skipping migration');
                return { success: true, migratedCount: 0 };
            } else {
                console.error('Error during migration:', error);
                return { success: false, error: error.message };
            }
        }
    }
}

module.exports = HistoryManager;