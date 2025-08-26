const XLSX = require('xlsx');
const natural = require('natural');
const fs = require('fs').promises;
const path = require('path');

// Configuration for fuzzy matching
const SIMILARITY_THRESHOLD = 0.8; // Minimum similarity score for matches
const ADDRESS_SIMILARITY_THRESHOLD = 0.8; // Higher threshold for address matching

class ExcelParser {
    constructor() {
        this.residentsData = new Map(); // Map: address -> residents array
        this.loadedFiles = new Set();
    }

    /**
     * Load all Excel files from the directory
     */
    async loadAllExcelFiles(directory = './') {
        try {
            const files = await fs.readdir(directory);
            const excelFiles = files.filter(file => 
                file.endsWith('.xlsx') || file.endsWith('.xls')
            );

            console.log(`Found ${excelFiles.length} Excel files`);

            for (const file of excelFiles) {
                await this.loadExcelFile(path.join(directory, file));
            }

            console.log(`Loaded data for ${this.residentsData.size} addresses`);
        } catch (error) {
            console.error('Error loading Excel files:', error);
        }
    }

    /**
     * Load a single Excel file
     */
    async loadExcelFile(filePath) {
        try {
            if (this.loadedFiles.has(filePath)) {
                return; // Already loaded
            }

            console.log(`Loading Excel file: ${filePath}`);
            
            // Extract address from filename
            const fileName = path.basename(filePath, path.extname(filePath));
            const address = this.extractAddressFromFilename(fileName);
            
            if (!address) {
                console.warn(`Could not extract address from filename: ${fileName}`);
                return;
            }

            const workbook = XLSX.readFile(filePath);
            const firstSheetName = workbook.SheetNames[0];
            const worksheet = workbook.Sheets[firstSheetName];
            
            // Convert to JSON with headers
            const residents = XLSX.utils.sheet_to_json(worksheet);
            
            // Process and store residents data
            const processedResidents = residents.map(resident => this.processResidentData(resident));
            
            this.residentsData.set(address, processedResidents);
            this.loadedFiles.add(filePath);
            
            console.log(`Loaded ${processedResidents.length} residents for address: ${address}`);
        } catch (error) {
            console.error(`Error loading Excel file ${filePath}:`, error);
        }
    }

    /**
     * Extract address from filename
     */
    extractAddressFromFilename(fileName) {
        // Remove common prefixes/suffixes and clean up
        let address = fileName
            .replace(/\.xlsx?$/i, '')
            .replace(/^(ул\.|улица|д\.|дом)/i, '')
            .trim();
        
        return address || null;
    }

    /**
     * Process resident data from Excel row
     */
    processResidentData(resident) {
        return {
            apartmentNumber: resident['Номер квартиры '] || resident['Номер квартиры'],
            accountNumber: resident['Л.С '] || resident['Л.С'] || resident['Лицевой счет'],
            lastName: resident['Фамилия'] || '',
            firstName: resident['Имя'] || '',
            middleName: resident['Отчество'] || '',
            fullName: `${resident['Фамилия'] || ''} ${resident['Имя'] || ''} ${resident['Отчество'] || ''}`.trim(),
            area: resident['Площадь помещения'],
            floor: resident['Этаж'],
            entrance: resident['Номер подъезда'],
            cadastralNumber: resident['Кадастровый номер']
        };
    }

    /**
     * Find resident account by name and address with fuzzy matching
     */
    findResidentAccount(fullName, address) {
        console.log(`Searching for: ${fullName} at ${address}`);
        
        // Find the best matching address
        const matchingAddress = this.findBestMatchingAddress(address);
        
        if (!matchingAddress) {
            console.log('No matching address found');
            return null;
        }

        console.log(`Best matching address: ${matchingAddress}`);
        
        const residents = this.residentsData.get(matchingAddress);
        if (!residents || residents.length === 0) {
            console.log('No residents data for this address');
            return null;
        }

        // Find the best matching resident by name
        const matchingResident = this.findBestMatchingResident(fullName, residents);
        
        if (matchingResident) {
            console.log(`Found matching resident: ${matchingResident.resident.fullName}, Account: ${matchingResident.resident.accountNumber}`);
            return {
                accountNumber: matchingResident.resident.accountNumber,
                fullName: matchingResident.resident.fullName,
                apartmentNumber: matchingResident.resident.apartmentNumber,
                address: matchingAddress,
                similarity: matchingResident.similarity
            };
        }

        console.log('No matching resident found');
        return null;
    }

    /**
     * Find best matching address using fuzzy string matching
     */
    findBestMatchingAddress(targetAddress) {
        let bestMatch = null;
        let bestSimilarity = 0;

        // Normalize the target address for better matching
        const normalizedTarget = this.normalizeAddress(targetAddress);

        for (const address of this.residentsData.keys()) {
            const normalizedAddress = this.normalizeAddress(address);
            const similarity = this.calculateStringSimilarity(normalizedTarget, normalizedAddress);
            
            // Use a lower threshold for address matching since addresses can vary significantly
            if (similarity > bestSimilarity && similarity >= 0.6) {
                bestSimilarity = similarity;
                bestMatch = address;
            }
        }

        console.log(`Best address match: "${bestMatch}" with similarity: ${bestSimilarity}`);
        return bestMatch;
    }

    /**
     * Normalize address for better matching
     */
    normalizeAddress(address) {
        if (!address) return '';
        
        return address
            .toLowerCase()
            .replace(/[.,]/g, ' ')  // Replace punctuation with spaces
            .replace(/\s+/g, ' ')   // Normalize whitespace
            .replace(/магомет/g, 'м')  // Handle "Магомет" -> "М"
            .replace(/гаджиева?/g, 'гаджиева')  // Normalize "Гаджиева"
            .replace(/д\.?\s*/g, '')  // Remove "д." prefix
            .replace(/кв\.?\s*/g, '')  // Remove "кв." prefix
            .replace(/[а-я]$/g, '')  // Remove single letter suffixes like "а"
            .trim();
    }

    /**
     * Find best matching resident by name
     */
    findBestMatchingResident(targetName, residents) {
        let bestMatch = null;
        let bestSimilarity = 0;

        for (const resident of residents) {
            if (!resident.fullName) continue;
            
            const similarity = this.calculateNameSimilarity(targetName, resident.fullName);
            
            if (similarity > bestSimilarity && similarity >= SIMILARITY_THRESHOLD) {
                bestSimilarity = similarity;
                bestMatch = { resident, similarity };
            }
        }

        return bestMatch;
    }

    /**
     * Calculate similarity between two names (more sophisticated matching)
     */
    calculateNameSimilarity(name1, name2) {
        // Normalize names
        const normalize = (name) => name.toLowerCase().replace(/[^а-яё\s]/g, '').trim();
        
        const normalized1 = normalize(name1);
        const normalized2 = normalize(name2);
        
        // Direct similarity
        const directSimilarity = natural.JaroWinklerDistance(normalized1, normalized2);
        
        // Word-based similarity (for cases where word order might be different)
        const words1 = normalized1.split(/\s+/).filter(w => w.length > 1);
        const words2 = normalized2.split(/\s+/).filter(w => w.length > 1);
        
        // For better matching, require at least 2 words to match for partial names
        let wordMatches = 0;
        let matchedWords1 = new Set();
        let matchedWords2 = new Set();
        
        for (let i = 0; i < words1.length; i++) {
            for (let j = 0; j < words2.length; j++) {
                if (!matchedWords1.has(i) && !matchedWords2.has(j)) {
                    const wordSim = natural.JaroWinklerDistance(words1[i], words2[j]);
                    if (wordSim > 0.85) {
                        wordMatches++;
                        matchedWords1.add(i);
                        matchedWords2.add(j);
                        break;
                    }
                }
            }
        }
        
        const totalWords = Math.max(words1.length, words2.length);
        const wordSimilarity = totalWords > 0 ? wordMatches / totalWords : 0;
        
        // If we have partial names (2 words or less), require higher word match ratio
        if (words1.length <= 2 || words2.length <= 2) {
            const minWords = Math.min(words1.length, words2.length);
            if (wordMatches < minWords) {
                return Math.min(directSimilarity, wordSimilarity * 0.8);
            }
        }
        
        // Return the higher of the two similarities
        return Math.max(directSimilarity, wordSimilarity);
    }

    /**
     * Calculate similarity between two strings
     */
    calculateStringSimilarity(str1, str2) {
        const normalize = (str) => str.toLowerCase().replace(/[^а-яё0-9\s]/g, '').trim();
        return natural.JaroWinklerDistance(normalize(str1), normalize(str2));
    }

    /**
     * Get statistics about loaded data
     */
    getStatistics() {
        const stats = {
            totalAddresses: this.residentsData.size,
            totalResidents: 0,
            addressList: []
        };

        for (const [address, residents] of this.residentsData.entries()) {
            stats.totalResidents += residents.length;
            stats.addressList.push({
                address,
                residentCount: residents.length
            });
        }

        return stats;
    }
}

module.exports = ExcelParser;