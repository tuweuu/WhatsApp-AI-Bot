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
        
        console.log(`Looking for normalized target address: "${normalizedTarget}"`);

        for (const address of this.residentsData.keys()) {
            const normalizedAddress = this.normalizeAddress(address);
            
            // Try different matching approaches
            let similarity = 0;
            
            // 1. Direct string similarity
            const directSimilarity = this.calculateStringSimilarity(normalizedTarget, normalizedAddress);
            
            // 2. Check if key address components match (street name + house number)
            const targetParts = normalizedTarget.split(/\s+/).filter(p => p.length > 0);
            const addressParts = normalizedAddress.split(/\s+/).filter(p => p.length > 0);
            
            // Try to find matching street name and house number
            const hasMatchingStreetAndNumber = this.hasMatchingStreetAndNumber(targetParts, addressParts);
            
            if (hasMatchingStreetAndNumber) {
                // Boost similarity if street name and number match
                similarity = Math.max(directSimilarity, 0.8);
            } else {
                similarity = directSimilarity;
            }
            
            console.log(`Comparing "${normalizedTarget}" with "${normalizedAddress}": similarity = ${similarity}`);
            
            // Use balanced threshold - flexible but not too loose
            if (similarity > bestSimilarity && similarity >= 0.65) {
                bestSimilarity = similarity;
                bestMatch = address;
            }
        }

        console.log(`Best address match: "${bestMatch}" with similarity: ${bestSimilarity}`);
        return bestMatch;
    }

    /**
     * Check if target address has matching street name and house number
     */
    hasMatchingStreetAndNumber(targetParts, addressParts) {
        // Look for house numbers (numeric parts)
        const targetNumbers = targetParts.filter(p => /\d/.test(p));
        const addressNumbers = addressParts.filter(p => /\d/.test(p));
        
        // Must have at least one matching number
        const hasMatchingNumber = targetNumbers.some(tNum => 
            addressNumbers.some(aNum => {
                const tClean = tNum.replace(/[^\d]/g, '');
                const aClean = aNum.replace(/[^\d]/g, '');
                return tClean === aClean || (tClean.length > 0 && aClean.length > 0 && 
                    (tClean.includes(aClean) || aClean.includes(tClean)));
            })
        );
        
        if (!hasMatchingNumber) {
            return false;
        }
        
        // Look for matching street name parts (non-numeric)
        const targetStreets = targetParts.filter(p => !/^\d+[а-я]*$/.test(p) && p.length > 2);
        const addressStreets = addressParts.filter(p => !/^\d+[а-я]*$/.test(p) && p.length > 2);
        
        // Check if any street name parts match
        const hasMatchingStreet = targetStreets.some(tStreet => 
            addressStreets.some(aStreet => {
                const streetSimilarity = this.calculateStringSimilarity(tStreet, aStreet);
                return streetSimilarity > 0.75;
            })
        );
        
        return hasMatchingStreet;
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
            // Handle common street name variations
            .replace(/магомет\s*гаджиев/g, 'м гаджиев')  // Handle "Магомет Гаджиев" -> "М Гаджиев"
            .replace(/магомеда?\s*гаджиев/g, 'м гаджиев')  // Handle "Магомеда Гаджиев" -> "М Гаджиев"
            .replace(/гаджиева?/g, 'гаджиев')  // Normalize "Гаджиева"
            .replace(/а[\.\s]*кадыров/g, 'кадыров')  // Handle "А.Кадырова" -> "Кадырова"
            .replace(/ахмат[\.\s-]*кадыров/g, 'кадыров')  // Handle "Ахмат-Кадырова" -> "Кадырова"
            // Remove common prefixes
            .replace(/^(ул|улица)[\.\s]*/g, '')  // Remove "ул." or "улица" prefix
            .replace(/д\.?\s*/g, '')  // Remove "д." prefix
            .replace(/дом\s*/g, '')  // Remove "дом" prefix
            .replace(/кв\.?\s*/g, '')  // Remove "кв." prefix
            .replace(/квартира\s*/g, '')  // Remove "квартира" prefix
            .replace(/г\.?\s*/g, '')  // Remove "г." prefix
            .replace(/город\s*/g, '')  // Remove "город" prefix
            // Remove trailing single letters and normalize
            .replace(/[а-я]$/g, '')  // Remove single letter suffixes like "а"
            .replace(/\s+/g, ' ')   // Normalize whitespace again
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