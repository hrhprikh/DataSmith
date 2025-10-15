// API Configuration
const API_BASE_URL = 'http://localhost:8000';

// DOM Elements
const elements = {
    // AI Processing
    aiUpload: document.getElementById('ai-upload'),
    aiFileInput: document.getElementById('ai-file-input'),
    processBtn: document.getElementById('process-btn'),
    forceAi: document.getElementById('force-ai'),
    
    // Analysis
    analyzeUpload: document.getElementById('analyze-upload'),
    analyzeFileInput: document.getElementById('analyze-file-input'),
    analyzeBtn: document.getElementById('analyze-btn'),
    
    // CSV to JSON Conversion
    csvSmartUpload: document.getElementById('csv-smart-upload'),
    csvSmartInput: document.getElementById('csv-smart-input'),
    csvSmartBtn: document.getElementById('csv-smart-btn'),
    csvRegularUpload: document.getElementById('csv-regular-upload'),
    csvRegularInput: document.getElementById('csv-regular-input'),
    csvRegularBtn: document.getElementById('csv-regular-btn'),
    
    // JSON to CSV Conversion
    jsonUpload: document.getElementById('json-upload'),
    jsonInput: document.getElementById('json-input'),
    jsonConvertBtn: document.getElementById('json-convert-btn'),
    
    // Batch Conversion
    batchUpload: document.getElementById('batch-upload'),
    batchInput: document.getElementById('batch-input'),
    batchBtn: document.getElementById('batch-btn'),
    
    // AI Data Report
    aiReportUpload: document.getElementById('ai-report-upload'),
    aiReportInput: document.getElementById('ai-report-input'),
    aiReportBtn: document.getElementById('ai-report-btn'),
    aiSampleReportBtn: document.getElementById('ai-sample-report-btn'),
    
    // Log Processor
    logFileUpload: document.getElementById('log-file-upload'),
    logFileInput: document.getElementById('log-file-input'),
    processLogFileBtn: document.getElementById('process-log-file-btn'),
    logTextInput: document.getElementById('log-text-input'),
    processLogTextBtn: document.getElementById('process-log-text-btn'),
    
    // Log Column Selection
    logColumnUpload: document.getElementById('log-column-upload'),
    logColumnInput: document.getElementById('log-column-input'),
    columnSelectionContainer: document.getElementById('column-selection-container'),
    columnCategories: document.getElementById('column-categories'),
    selectAllColumnsBtn: document.getElementById('select-all-columns'),
    deselectAllColumnsBtn: document.getElementById('deselect-all-columns'),
    selectEssentialColumnsBtn: document.getElementById('select-essential-columns'),
    createFilteredCsvBtn: document.getElementById('create-filtered-csv-btn'),
    selectedCount: document.getElementById('selected-count'),
    
    // PDF Processor
    pdfCsvUpload: document.getElementById('pdf-csv-upload'),
    pdfCsvInput: document.getElementById('pdf-csv-input'),
    processPdfCsvBtn: document.getElementById('process-pdf-csv-btn'),
    pdfJsonUpload: document.getElementById('pdf-json-upload'),
    pdfJsonInput: document.getElementById('pdf-json-input'),
    processPdfJsonBtn: document.getElementById('process-pdf-json-btn'),
    pdfHealthCheckBtn: document.getElementById('pdf-health-check-btn'),
    
    // Options
    jsonFormat: document.getElementById('json-format'),
    prettyJson: document.getElementById('pretty-json'),
    autoFlatten: document.getElementById('auto-flatten'),
    batchFormat: document.getElementById('batch-format'),
    
    // LM Studio Connection
    connectionStatus: document.getElementById('connection-status'),
    lmStudioUrl: document.getElementById('lm-studio-url'),
    testConnectionBtn: document.getElementById('test-connection-btn'),
    modelSelect: document.getElementById('model-select'),
    refreshModelsBtn: document.getElementById('refresh-models-btn'),
    
    // UI Elements
    resultsContainer: document.getElementById('results-container'),
    loadingOverlay: document.getElementById('loading-overlay'),
    loadingText: document.getElementById('loading-text'),
    
    // Tab system
    tabBtns: document.querySelectorAll('.tab-btn'),
    tabContents: document.querySelectorAll('.tab-content')
};

// Global State
let currentFiles = {};
let currentAIReport = null;

// Initialize App
document.addEventListener('DOMContentLoaded', function() {
    console.log('DOM loaded, initializing app...');
    
    // Debug: Check if key elements exist
    console.log('PDF CSV upload area:', document.getElementById('pdf-csv-upload'));
    console.log('PDF CSV input:', document.getElementById('pdf-csv-input'));
    console.log('PDF JSON upload area:', document.getElementById('pdf-json-upload'));
    console.log('PDF JSON input:', document.getElementById('pdf-json-input'));
    
    initializeEventListeners();
    initializeTabs();
    checkAPIConnection();
});

// Event Listeners Setup
function initializeEventListeners() {
    // File upload areas
    setupFileUpload('ai-upload', 'ai-file-input', handleAIFileSelect);
    setupFileUpload('analyze-upload', 'analyze-file-input', handleAnalyzeFileSelect);
    setupFileUpload('csv-smart-upload', 'csv-smart-input', handleCsvSmartFileSelect);
    setupFileUpload('csv-regular-upload', 'csv-regular-input', handleCsvRegularFileSelect);
    setupFileUpload('json-upload', 'json-input', handleJsonFileSelect);
    setupFileUpload('batch-upload', 'batch-input', handleBatchFileSelect);
    setupFileUpload('ai-report-upload', 'ai-report-input', handleAIReportFileSelect);
    setupFileUpload('log-file-upload', 'log-file-input', handleLogFileSelect);
    setupFileUpload('log-column-upload', 'log-column-input', handleLogColumnFileSelect);
    setupFileUpload('pdf-csv-upload', 'pdf-csv-input', handlePdfCsvFileSelect);
    setupFileUpload('pdf-json-upload', 'pdf-json-input', handlePdfJsonFileSelect);
    
    // Action buttons
    elements.processBtn?.addEventListener('click', processWithAI);
    elements.analyzeBtn?.addEventListener('click', analyzeData);
    elements.csvSmartBtn?.addEventListener('click', convertCsvToJsonSmart);
    elements.csvRegularBtn?.addEventListener('click', convertCsvToJson);
    elements.jsonConvertBtn?.addEventListener('click', convertJsonToCsv);
    elements.batchBtn?.addEventListener('click', batchConvert);
    elements.aiReportBtn?.addEventListener('click', generateAIReport);
    elements.aiSampleReportBtn?.addEventListener('click', viewSampleAIReport);
    elements.processLogFileBtn?.addEventListener('click', processLogFile);
    elements.processLogTextBtn?.addEventListener('click', processLogText);
    
    // LM Studio connection buttons
    elements.testConnectionBtn?.addEventListener('click', testLMStudioConnection);
    elements.refreshModelsBtn?.addEventListener('click', refreshAvailableModels);
    elements.lmStudioUrl?.addEventListener('change', saveLMStudioSettings);
    elements.modelSelect?.addEventListener('change', saveLMStudioSettings);
    
    // Log column selection buttons
    elements.selectAllColumnsBtn?.addEventListener('click', selectAllColumns);
    elements.deselectAllColumnsBtn?.addEventListener('click', deselectAllColumns);
    elements.selectEssentialColumnsBtn?.addEventListener('click', selectEssentialColumns);
    elements.createFilteredCsvBtn?.addEventListener('click', createFilteredCsv);
    
    // PDF processing buttons
    elements.processPdfCsvBtn?.addEventListener('click', processPdfToCsv);
    elements.processPdfJsonBtn?.addEventListener('click', processPdfToJson);
    elements.pdfHealthCheckBtn?.addEventListener('click', checkPdfHealth);
    elements.processPdfCsvBtn?.addEventListener('click', processPdfToCsv);
    elements.processPdfJsonBtn?.addEventListener('click', processPdfToJson);
    elements.pdfHealthCheckBtn?.addEventListener('click', checkPdfHealthStatus);
    
    // Initialize button states
    if (elements.processLogFileBtn) {
        elements.processLogFileBtn.disabled = true;
        console.log('Log file button initialized as disabled');
    }
    
    if (elements.aiReportBtn) {
        elements.aiReportBtn.disabled = true;
        console.log('AI Report button initialized as disabled');
    }
}

// File Upload Setup
function setupFileUpload(uploadId, inputId, handler) {
    const uploadArea = document.getElementById(uploadId);
    const fileInput = document.getElementById(inputId);
    
    console.log(`Setting up file upload for: ${uploadId}, ${inputId}`);
    console.log('Upload area found:', uploadArea);
    console.log('File input found:', fileInput);
    
    if (!uploadArea || !fileInput) {
        console.error(`Missing elements for ${uploadId}: uploadArea=${uploadArea}, fileInput=${fileInput}`);
        return;
    }
    
    // Click to upload
    uploadArea.addEventListener('click', () => {
        console.log(`Upload area clicked for ${uploadId}`);
        fileInput.click();
    });
    
    // File selection
    fileInput.addEventListener('change', (e) => {
        console.log('File input change event:', e.target.files);
        handler(e);
    });
    
    // Drag and drop
    uploadArea.addEventListener('dragover', (e) => {
        e.preventDefault();
        uploadArea.classList.add('dragover');
    });
    
    uploadArea.addEventListener('dragleave', () => {
        uploadArea.classList.remove('dragover');
    });
    
    uploadArea.addEventListener('drop', (e) => {
        e.preventDefault();
        uploadArea.classList.remove('dragover');
        
        const files = Array.from(e.dataTransfer.files);
        if (files.length > 0) {
            fileInput.files = e.dataTransfer.files;
            console.log('File drop event:', e.dataTransfer.files);
            // Create a fake event object for consistency
            handler({ target: { files: e.dataTransfer.files } });
        }
    });
}

// File Selection Handlers
function handleAIFileSelect(event) {
    const file = event.target.files[0];
    if (file) {
        currentFiles.aiFile = file;
        updateUploadArea('ai-upload', file);
        elements.processBtn.disabled = false;
    }
}

function handleAnalyzeFileSelect(event) {
    const file = event.target.files[0];
    if (file) {
        currentFiles.analyzeFile = file;
        updateUploadArea('analyze-upload', file);
        elements.analyzeBtn.disabled = false;
    }
}

function handleCsvSmartFileSelect(event) {
    const file = event.target.files[0];
    if (file) {
        currentFiles.csvSmartFile = file;
        updateUploadArea('csv-smart-upload', file);
        elements.csvSmartBtn.disabled = false;
    }
}

function handleCsvRegularFileSelect(event) {
    const file = event.target.files[0];
    if (file) {
        currentFiles.csvRegularFile = file;
        updateUploadArea('csv-regular-upload', file);
        elements.csvRegularBtn.disabled = false;
    }
}

function handleJsonFileSelect(event) {
    const file = event.target.files[0];
    if (file) {
        currentFiles.jsonFile = file;
        updateUploadArea('json-upload', file);
        elements.jsonConvertBtn.disabled = false;
    }
}

function handleBatchFileSelect(event) {
    const files = Array.from(event.target.files);
    if (files.length > 0) {
        currentFiles.batchFiles = files;
        updateUploadArea('batch-upload', files);
        elements.batchBtn.disabled = false;
    }
}

// Update Upload Area Display
function updateUploadArea(uploadId, files) {
    const uploadArea = document.getElementById(uploadId);
    if (!uploadArea) return;
    
    const icon = uploadArea.querySelector('i');
    const span = uploadArea.querySelector('span');
    
    if (!icon || !span) return;
    
    if (typeof files === 'string') {
        // String message
        icon.className = 'fas fa-check-circle';
        icon.style.color = '#28a745';
        span.textContent = files;
    } else if (Array.isArray(files)) {
        // Array of files
        icon.className = 'fas fa-check-circle';
        icon.style.color = '#28a745';
        span.textContent = `${files.length} files selected`;
    } else if (files && files.name) {
        // Single file object
        icon.className = 'fas fa-check-circle';
        icon.style.color = '#28a745';
        span.textContent = files.name;
    }
}

// API Functions
async function processWithAI() {
    if (!currentFiles.aiFile) return;
    
    showLoading('Processing with AI...');
    
    try {
        const formData = new FormData();
        formData.append('file', currentFiles.aiFile);
        formData.append('force_ai', elements.forceAi.checked);
        
        const response = await fetch(`${API_BASE_URL}/process/`, {
            method: 'POST',
            body: formData
        });
        
        const result = await response.json();
        
        if (response.ok) {
            displayResult('AI Processing', result, 'success');
        } else {
            throw new Error(result.detail || 'Processing failed');
        }
    } catch (error) {
        displayResult('AI Processing', { error: error.message }, 'error');
    } finally {
        hideLoading();
    }
}

async function analyzeData() {
    if (!currentFiles.analyzeFile) return;
    
    showLoading('Analyzing data...');
    
    try {
        const formData = new FormData();
        formData.append('file', currentFiles.analyzeFile);
        
        const response = await fetch(`${API_BASE_URL}/analyze/`, {
            method: 'POST',
            body: formData
        });
        
        const result = await response.json();
        
        if (response.ok) {
            displayResult('Data Analysis', result, 'success');
        } else {
            throw new Error(result.detail || 'Analysis failed');
        }
    } catch (error) {
        displayResult('Data Analysis', { error: error.message }, 'error');
    } finally {
        hideLoading();
    }
}

async function convertCsvToJsonSmart() {
    if (!currentFiles.csvSmartFile) return;
    
    showLoading('Converting CSV to JSON with AI headers...');
    
    try {
        const formData = new FormData();
        formData.append('file', currentFiles.csvSmartFile);
        
        const params = new URLSearchParams({
            format_type: elements.jsonFormat.value,
            pretty: elements.prettyJson.checked
        });
        
        const response = await fetch(`${API_BASE_URL}/convert/csv-to-json-smart/?${params}`, {
            method: 'POST',
            body: formData
        });
        
        const result = await response.json();
        
        if (response.ok) {
            displayResult('Smart CSV → JSON Conversion', result, 'success');
        } else {
            throw new Error(result.detail || 'Conversion failed');
        }
    } catch (error) {
        displayResult('Smart CSV → JSON Conversion', { error: error.message }, 'error');
    } finally {
        hideLoading();
    }
}

async function convertCsvToJson() {
    if (!currentFiles.csvRegularFile) return;
    
    showLoading('Converting CSV to JSON...');
    
    try {
        const formData = new FormData();
        formData.append('file', currentFiles.csvRegularFile);
        
        const params = new URLSearchParams({
            format_type: elements.jsonFormat.value,
            pretty: elements.prettyJson.checked
        });
        
        const response = await fetch(`${API_BASE_URL}/convert/csv-to-json/?${params}`, {
            method: 'POST',
            body: formData
        });
        
        const result = await response.json();
        
        if (response.ok) {
            displayResult('CSV → JSON Conversion', result, 'success');
        } else {
            throw new Error(result.detail || 'Conversion failed');
        }
    } catch (error) {
        displayResult('CSV → JSON Conversion', { error: error.message }, 'error');
    } finally {
        hideLoading();
    }
}

async function convertJsonToCsv() {
    if (!currentFiles.jsonFile) return;
    
    showLoading('Converting JSON to CSV...');
    
    try {
        const formData = new FormData();
        formData.append('file', currentFiles.jsonFile);
        
        const params = new URLSearchParams({
            auto_flatten: elements.autoFlatten.checked
        });
        
        const response = await fetch(`${API_BASE_URL}/convert/json-to-csv/?${params}`, {
            method: 'POST',
            body: formData
        });
        
        const result = await response.json();
        
        if (response.ok) {
            displayResult('JSON → CSV Conversion', result, 'success');
        } else {
            throw new Error(result.detail || 'Conversion failed');
        }
    } catch (error) {
        displayResult('JSON → CSV Conversion', { error: error.message }, 'error');
    } finally {
        hideLoading();
    }
}

async function batchConvert() {
    if (!currentFiles.batchFiles || currentFiles.batchFiles.length === 0) return;
    
    showLoading(`Converting ${currentFiles.batchFiles.length} files...`);
    
    try {
        const formData = new FormData();
        currentFiles.batchFiles.forEach(file => {
            formData.append('files', file);
        });
        
        const params = new URLSearchParams({
            target_format: elements.batchFormat.value
        });
        
        const response = await fetch(`${API_BASE_URL}/convert/batch/?${params}`, {
            method: 'POST',
            body: formData
        });
        
        const result = await response.json();
        
        if (response.ok) {
            displayResult('Batch Conversion', result, 'success');
        } else {
            throw new Error(result.detail || 'Batch conversion failed');
        }
    } catch (error) {
        displayResult('Batch Conversion', { error: error.message }, 'error');
    } finally {
        hideLoading();
    }
}

// UI Helper Functions
function showLoading(message = 'Processing...') {
    elements.loadingText.textContent = message;
    elements.loadingOverlay.classList.remove('hidden');
}

function hideLoading() {
    elements.loadingOverlay.classList.add('hidden');
}

function showError(message) {
    console.error('Error:', message);
    
    // Create or update error message display
    let errorContainer = document.getElementById('error-container');
    if (!errorContainer) {
        errorContainer = document.createElement('div');
        errorContainer.id = 'error-container';
        errorContainer.className = 'message message-error';
        errorContainer.style.cssText = `
            position: fixed;
            top: 20px;
            right: 20px;
            background: #f8d7da;
            color: #721c24;
            border: 1px solid #f5c6cb;
            padding: 15px 20px;
            border-radius: 8px;
            box-shadow: 0 4px 15px rgba(0, 0, 0, 0.1);
            z-index: 1000;
            max-width: 400px;
            word-wrap: break-word;
        `;
        document.body.appendChild(errorContainer);
    }
    
    errorContainer.innerHTML = `<strong>Error:</strong> ${message}`;
    errorContainer.style.display = 'block';
    
    // Auto-hide after 5 seconds
    setTimeout(() => {
        if (errorContainer) {
            errorContainer.style.display = 'none';
        }
    }, 5000);
}

function showSuccess(message) {
    console.log('Success:', message);
    
    // Create or update success message display
    let successContainer = document.getElementById('success-container');
    if (!successContainer) {
        successContainer = document.createElement('div');
        successContainer.id = 'success-container';
        successContainer.className = 'message message-success';
        successContainer.style.cssText = `
            position: fixed;
            top: 20px;
            right: 20px;
            background: #d4edda;
            color: #155724;
            border: 1px solid #c3e6cb;
            padding: 15px 20px;
            border-radius: 8px;
            box-shadow: 0 4px 15px rgba(0, 0, 0, 0.1);
            z-index: 1000;
            max-width: 400px;
            word-wrap: break-word;
        `;
        document.body.appendChild(successContainer);
    }
    
    successContainer.innerHTML = `<strong>Success:</strong> ${message}`;
    successContainer.style.display = 'block';
    
    // Auto-hide after 3 seconds
    setTimeout(() => {
        if (successContainer) {
            successContainer.style.display = 'none';
        }
    }, 3000);
}

function showMessage(message, type = 'info') {
    console.log(`${type.toUpperCase()}:`, message);
    
    // Create or update message display
    let messageContainer = document.getElementById('message-container');
    if (!messageContainer) {
        messageContainer = document.createElement('div');
        messageContainer.id = 'message-container';
        messageContainer.style.cssText = `
            position: fixed;
            top: 20px;
            right: 20px;
            padding: 15px 20px;
            border-radius: 8px;
            box-shadow: 0 4px 15px rgba(0, 0, 0, 0.1);
            z-index: 1000;
            max-width: 400px;
            word-wrap: break-word;
        `;
        document.body.appendChild(messageContainer);
    }
    
    // Set style based on type
    switch (type) {
        case 'success':
            messageContainer.className = 'message message-success';
            messageContainer.style.background = '#d4edda';
            messageContainer.style.color = '#155724';
            messageContainer.style.border = '1px solid #c3e6cb';
            break;
        case 'error':
            messageContainer.className = 'message message-error';
            messageContainer.style.background = '#f8d7da';
            messageContainer.style.color = '#721c24';
            messageContainer.style.border = '1px solid #f5c6cb';
            break;
        case 'warning':
            messageContainer.className = 'message message-warning';
            messageContainer.style.background = '#fff3cd';
            messageContainer.style.color = '#856404';
            messageContainer.style.border = '1px solid #ffeaa7';
            break;
        default: // info
            messageContainer.className = 'message message-info';
            messageContainer.style.background = '#d1ecf1';
            messageContainer.style.color = '#0c5460';
            messageContainer.style.border = '1px solid #bee5eb';
    }
    
    messageContainer.innerHTML = message;
    messageContainer.style.display = 'block';
    
    // Auto-hide after 4 seconds
    setTimeout(() => {
        if (messageContainer) {
            messageContainer.style.display = 'none';
        }
    }, 4000);
}

function displayResult(title, result, type) {
    // Remove empty state if it exists
    const emptyState = elements.resultsContainer.querySelector('.empty-state');
    if (emptyState) {
        emptyState.remove();
    }
    
    // Create result card
    const resultCard = document.createElement('div');
    resultCard.className = 'result-card';
    
    let content = `
        <div class="result-header">
            <h3 class="result-title">${title}</h3>
            <span class="result-status status-${type}">
                ${type === 'success' ? '✅ Success' : '❌ Error'}
            </span>
        </div>
    `;
    
    if (type === 'error') {
        content += `
            <div class="message message-error">
                <strong>Error:</strong> ${result.error}
            </div>
        `;
    } else {
        // Success result
        if (result.conversion_info) {
            const info = result.conversion_info;
            content += `
                <div class="result-details">
                    <div class="result-item">
                        <span>Input File:</span>
                        <strong>${info.input_file}</strong>
                    </div>
                    <div class="result-item">
                        <span>Output File:</span>
                        <strong>${info.output_file}</strong>
                    </div>
                    <div class="result-item">
                        <span>Records:</span>
                        <strong>${info.records_count || 'N/A'}</strong>
                    </div>
                    <div class="result-item">
                        <span>Columns:</span>
                        <strong>${info.columns_count || 'N/A'}</strong>
                    </div>
                </div>
            `;
            
            // Show AI insights if available
            if (info.ai_generated_headers) {
                content += `
                    <div class="message message-info">
                        <strong>🤖 AI Enhancement:</strong> 
                        Generated intelligent headers: ${info.final_columns ? info.final_columns.join(', ') : 'N/A'}
                    </div>
                `;
            }
            
            // Download button
            if (info.output_file) {
                const fileType = info.output_file.endsWith('.json') ? 'json' : 'csv';
                content += `
                    <a href="${API_BASE_URL}/convert/download/${fileType}/${info.output_file}" 
                       class="download-btn" target="_blank">
                        <i class="fas fa-download"></i>
                        Download ${info.output_file}
                    </a>
                `;
            }
        } else if (result.file_info) {
            // AI Processing result
            const info = result.file_info;
            content += `
                <div class="result-details">
                    <div class="result-item">
                        <span>Original:</span>
                        <strong>${info.original_name}</strong>
                    </div>
                    <div class="result-item">
                        <span>Processed:</span>
                        <strong>${info.processed_name}</strong>
                    </div>
                    <div class="result-item">
                        <span>Records:</span>
                        <strong>${info.rows_processed}</strong>
                    </div>
                    <div class="result-item">
                        <span>Columns:</span>
                        <strong>${info.columns.length}</strong>
                    </div>
                </div>
            `;
            
            if (result.ai_insights) {
                content += `
                    <div class="message message-info">
                        <strong>🧠 AI Insights:</strong><br>
                        Data Type: ${result.ai_insights.data_type}<br>
                        ${result.ai_insights.label_explanation}
                    </div>
                `;
            }
        } else if (result.successful_conversions !== undefined) {
            // Batch conversion result
            content += `
                <div class="result-details">
                    <div class="result-item">
                        <span>Successful:</span>
                        <strong>${result.successful_conversions}</strong>
                    </div>
                    <div class="result-item">
                        <span>Failed:</span>
                        <strong>${result.failed_conversions}</strong>
                    </div>
                </div>
            `;
            
            if (result.results && result.results.length > 0) {
                content += `<div class="message message-success">
                    <strong>Converted files:</strong><br>
                    ${result.results.map(r => `• ${r.file}`).join('<br>')}
                </div>`;
            }
        } else {
            // Analysis result
            content += `
                <div class="result-details">
                    <div class="result-item">
                        <span>Rows:</span>
                        <strong>${result.dimensions?.rows || 'N/A'}</strong>
                    </div>
                    <div class="result-item">
                        <span>Columns:</span>
                        <strong>${result.dimensions?.columns || 'N/A'}</strong>
                    </div>
                    <div class="result-item">
                        <span>Completeness:</span>
                        <strong>${result.data_quality?.completeness || 'N/A'}</strong>
                    </div>
                </div>
            `;
        }
    }
    
    resultCard.innerHTML = content;
    elements.resultsContainer.prepend(resultCard);
    
    // Scroll to results
    resultCard.scrollIntoView({ behavior: 'smooth' });
}

function showResults(options) {
    const { title, message, data, isHtml = false } = options;
    
    // Remove empty state if it exists
    const emptyState = elements.resultsContainer.querySelector('.empty-state');
    if (emptyState) {
        emptyState.remove();
    }
    
    // Create result card
    const resultCard = document.createElement('div');
    resultCard.className = 'result-card';
    
    let content = `
        <div class="result-header">
            <h3 class="result-title">${title}</h3>
            <span class="result-status status-success">
                ✅ Success
            </span>
        </div>
    `;
    
    if (message) {
        content += `
            <div class="message message-success">
                ${message}
            </div>
        `;
    }
    
    if (data) {
        if (isHtml) {
            content += `<div class="result-content">${data}</div>`;
        } else {
            content += `
                <div class="result-content">
                    <pre style="background: #f8f9fa; padding: 15px; border-radius: 8px; overflow-x: auto; white-space: pre-wrap;">
                        ${typeof data === 'object' ? JSON.stringify(data, null, 2) : data}
                    </pre>
                </div>
            `;
        }
    }
    
    resultCard.innerHTML = content;
    elements.resultsContainer.prepend(resultCard);
    
    // Scroll to results
    resultCard.scrollIntoView({ behavior: 'smooth' });
}

// Tab System
function initializeTabs() {
    elements.tabBtns.forEach(btn => {
        btn.addEventListener('click', () => {
            const targetTab = btn.getAttribute('data-tab');
            switchTab(targetTab);
        });
    });
}

function switchTab(targetTab) {
    // Update buttons
    elements.tabBtns.forEach(btn => {
        if (btn.getAttribute('data-tab') === targetTab) {
            btn.classList.add('active');
        } else {
            btn.classList.remove('active');
        }
    });
    
    // Update content
    elements.tabContents.forEach(content => {
        if (content.id === targetTab) {
            content.classList.add('active');
        } else {
            content.classList.remove('active');
        }
    });
}

// API Connection Check
async function checkAPIConnection() {
    try {
        const response = await fetch(`${API_BASE_URL}/`);
        if (response.ok) {
            console.log('✅ API connection successful');
        } else {
            throw new Error('API not responding');
        }
    } catch (error) {
        console.error('❌ API connection failed:', error);
        displayResult('System Status', { 
            error: 'Cannot connect to API. Make sure the server is running on http://localhost:8000' 
        }, 'error');
    }
}

// Utility Functions
function formatFileSize(bytes) {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

function validateFileType(file, allowedTypes) {
    const fileExtension = file.name.split('.').pop().toLowerCase();
    return allowedTypes.includes(fileExtension);
}

// Smooth scrolling for navigation links
document.querySelectorAll('a[href^="#"]').forEach(anchor => {
    anchor.addEventListener('click', function (e) {
        e.preventDefault();
        const targetId = this.getAttribute('href').substring(1);
        const targetElement = document.getElementById(targetId);
        
        if (targetElement) {
            targetElement.scrollIntoView({
                behavior: 'smooth',
                block: 'start'
            });
        }
    });
});

// AI Data Understanding Functions
let selectedAIReportFile = null;

function handleAIReportFileSelect(event) {
    console.log('AI Report file select event triggered:', event);
    const files = event.target.files;
    console.log('Files selected:', files);
    
    if (files && files.length > 0) {
        selectedAIReportFile = files[0];
        console.log('Selected AI report file:', selectedAIReportFile);
        const fileName = selectedAIReportFile.name.toLowerCase();
        
        if (fileName.endsWith('.csv') || fileName.endsWith('.json')) {
            updateUploadArea('ai-report-upload', `Selected: ${selectedAIReportFile.name}`);
            elements.aiReportBtn.disabled = false;
            console.log('AI Report button enabled');
        } else {
            showError('Please select a CSV or JSON file for AI analysis.');
            selectedAIReportFile = null;
            elements.aiReportBtn.disabled = true;
            console.log('Invalid file type, button disabled');
        }
    } else {
        console.log('No files selected');
    }
}

async function generateAIReport() {
    if (!selectedAIReportFile) {
        showError('Please select a CSV or JSON file first.');
        return;
    }

    const fileName = selectedAIReportFile.name.toLowerCase();
    const isCSV = fileName.endsWith('.csv');
    const isJSON = fileName.endsWith('.json');
    
    if (!isCSV && !isJSON) {
        showError('Please select a valid CSV or JSON file.');
        return;
    }

    showLoading(`Generating AI Data Analysis Report...`);
    
    try {
        const formData = new FormData();
        formData.append('file', selectedAIReportFile);
        
        const endpoint = isCSV ? '/ai-report/csv' : '/ai-report/json';
        
        const response = await fetch(`${API_BASE_URL}${endpoint}`, {
            method: 'POST',
            body: formData
        });
        
        const result = await response.json();
        
        if (!response.ok) {
            throw new Error(result.detail || 'Failed to generate AI report');
        }
        
        hideLoading();
        displayAIReport(result.report, selectedAIReportFile.name, result.report_type);
        
    } catch (error) {
        hideLoading();
        console.error('AI Report Error:', error);
        showError(`Failed to generate AI report: ${error.message}`);
    }
}

async function viewSampleAIReport() {
    showLoading('Generating sample AI report...');
    
    try {
        const response = await fetch(`${API_BASE_URL}/ai-report/test-csv`);
        const result = await response.json();
        
        if (!response.ok) {
            throw new Error(result.detail || 'Failed to generate sample report');
        }
        
        hideLoading();
        displayAIReport(result.report, 'Sample Data', 'csv_analysis');
        
    } catch (error) {
        hideLoading();
        showError('Sample Report Error: ' + error.message);
    }
}

function displayAIReport(report, filename, reportType) {
    const fileType = reportType === 'csv_analysis' ? 'CSV' : 'JSON';
    const reportHtml = `
        <div class="ai-report">
            <div class="report-header">
                <h3>🤖 AI Data Analysis Report</h3>
                <p><strong>Dataset:</strong> ${filename} (${fileType})</p>
                <p><strong>Generated:</strong> ${new Date(report.file_info.analysis_timestamp).toLocaleString()}</p>
                <p><strong>Size:</strong> ${report.file_info.total_rows.toLocaleString()} rows × ${report.file_info.total_columns} columns</p>
            </div>
            
            <div class="report-section">
                <h4>📊 Executive Summary</h4>
                <div class="metrics-grid">
                    <div class="metric">
                        <strong>Data Quality Score:</strong> ${report.summary.data_quality_score}
                    </div>
                    <div class="metric">
                        <strong>Data Completeness:</strong> ${report.file_info.data_density.toFixed(1)}%
                    </div>
                    <div class="metric">
                        <strong>Business Value:</strong> ${report.summary.business_value}
                    </div>
                    <div class="metric">
                        <strong>File Size:</strong> ${report.file_info.file_size_estimate}
                    </div>
                </div>
                <div class="summary-overview">
                    <p><strong>Overview:</strong> ${report.summary.data_overview}</p>
                    <div class="characteristics">
                        <strong>Key Characteristics:</strong>
                        <ul>
                            ${report.summary.key_characteristics.map(char => `<li>${char}</li>`).join('')}
                        </ul>
                    </div>
                </div>
            </div>
            
            ${report.data_quality ? `
            <div class="report-section">
                <h4>🔍 Data Quality Analysis</h4>
                <div class="quality-metrics">
                    <div class="quality-score">
                        <strong>Quality Grade:</strong> ${report.data_quality.quality_grade.toFixed(1)}%
                    </div>
                    <div class="completeness">
                        <strong>Completeness:</strong> ${report.data_quality.completeness_score.toFixed(1)}%
                    </div>
                    <div class="consistency">
                        <strong>Consistency:</strong> ${report.data_quality.consistency_score.toFixed(1)}%
                    </div>
                </div>
                ${report.data_quality.data_quality_issues.length > 0 ? `
                <div class="quality-issues">
                    <strong>Data Quality Issues:</strong>
                    <ul>
                        ${report.data_quality.data_quality_issues.map(issue => `<li>${issue}</li>`).join('')}
                    </ul>
                </div>` : '<p class="no-issues">✅ No significant data quality issues detected.</p>'}
            </div>` : ''}
            
            ${report.data_structure ? `
            <div class="report-section">
                <h4>📈 Data Structure Analysis</h4>
                <div class="structure-summary">
                    <div class="data-types">
                        <strong>Data Types Distribution:</strong>
                        <ul>
                            ${Object.entries(report.data_structure.data_types_summary).map(([type, count]) => 
                                `<li>${type}: ${count} columns</li>`
                            ).join('')}
                        </ul>
                    </div>
                    <div class="column-categories">
                        <p><strong>Numeric Columns:</strong> ${report.data_structure.numeric_columns.length}</p>
                        <p><strong>Categorical Columns:</strong> ${report.data_structure.categorical_columns.length}</p>
                        <p><strong>DateTime Columns:</strong> ${report.data_structure.datetime_columns.length}</p>
                    </div>
                </div>
            </div>` : ''}
            
            ${report.statistical_insights ? `
            <div class="report-section">
                <h4>📊 Statistical Insights</h4>
                ${report.statistical_insights.correlations && report.statistical_insights.correlations.length > 0 ? `
                <div class="correlations">
                    <strong>Strong Correlations Detected:</strong>
                    <ul>
                        ${report.statistical_insights.correlations.map(corr => 
                            `<li>${corr.column1} ↔ ${corr.column2}: ${(corr.correlation * 100).toFixed(1)}%</li>`
                        ).join('')}
                    </ul>
                </div>` : ''}
                ${report.statistical_insights.categorical_summary ? `
                <div class="categorical-insights">
                    <strong>Categorical Data Summary:</strong>
                    <div class="categorical-grid">
                        ${Object.entries(report.statistical_insights.categorical_summary).slice(0, 3).map(([col, info]) => `
                            <div class="categorical-item">
                                <strong>${col}:</strong>
                                <br>Unique values: ${info.unique_count}
                                <br>Cardinality: ${info.cardinality}
                            </div>
                        `).join('')}
                    </div>
                </div>` : ''}
            </div>` : ''}
            
            ${report.pattern_analysis && Object.keys(report.pattern_analysis).length > 0 ? `
            <div class="report-section">
                <h4>🔍 Pattern Analysis</h4>
                ${report.pattern_analysis.temporal_patterns ? `
                <div class="temporal-patterns">
                    <strong>Temporal Patterns:</strong>
                    <p>Time range: ${report.pattern_analysis.temporal_patterns.time_range.start} to ${report.pattern_analysis.temporal_patterns.time_range.end}</p>
                    <p>Duration: ${report.pattern_analysis.temporal_patterns.time_range.duration}</p>
                </div>` : ''}
                ${report.pattern_analysis.network_patterns ? `
                <div class="network-patterns">
                    <strong>Network Patterns:</strong>
                    <p>Unique IPs: ${report.pattern_analysis.network_patterns.unique_ips}</p>
                    <p>IP Diversity: ${(report.pattern_analysis.network_patterns.ip_diversity * 100).toFixed(1)}%</p>
                    <p>Private IPs: ${report.pattern_analysis.network_patterns.private_ip_percentage.toFixed(1)}%</p>
                </div>` : ''}
            </div>` : ''}
            
            <div class="report-section ai-insights">
                <h4>🧠 AI-Powered Insights</h4>
                <div class="insights-list">
                    ${report.ai_insights.map(insight => `<div class="insight-item">${insight}</div>`).join('')}
                </div>
            </div>
            
            <div class="report-section recommendations">
                <h4>💡 AI Recommendations</h4>
                <div class="recommendations-list">
                    ${report.recommendations.map(rec => `<div class="recommendation-item">${rec}</div>`).join('')}
                </div>
            </div>
            
            <div class="report-section next-steps">
                <h4>🚀 Next Steps</h4>
                <div class="next-steps-list">
                    ${report.summary.next_steps.map(step => `<div class="step-item">${step}</div>`).join('')}
                </div>
            </div>
            
            <div class="report-section download-section">
                <h4>📥 Export Report</h4>
                <div class="download-buttons">
                    <button class="btn btn-primary" onclick="downloadAIReport('${filename}', 'json')">
                        <i class="fas fa-download"></i>
                        Download JSON Report
                    </button>
                    <button class="btn btn-secondary" onclick="downloadAIReport('${filename}', 'html')">
                        <i class="fas fa-file-code"></i>
                        Download HTML Report
                    </button>
                </div>
            </div>
        </div>
    `;
    
    // Store report data for download
    currentAIReport = {
        filename: filename,
        reportType: reportType,
        data: report,
        timestamp: new Date().toISOString()
    };
    
    showResults({
        title: `🤖 AI Data Analysis Report - ${filename}`,
        message: `Comprehensive AI analysis completed for ${fileType} file`,
        data: reportHtml,
        isHtml: true
    });
}

function downloadAIReport(filename, format) {
    if (!currentAIReport) {
        showError('No report data available for download.');
        return;
    }
    
    let downloadData, mimeType, fileExtension;
    
    if (format === 'json') {
        // Download as JSON
        downloadData = JSON.stringify(currentAIReport.data, null, 2);
        mimeType = 'application/json';
        fileExtension = 'json';
    } else if (format === 'html') {
        // Generate HTML version
        const htmlContent = generateHTMLReport(currentAIReport.data, filename);
        downloadData = htmlContent;
        mimeType = 'text/html';
        fileExtension = 'html';
    } else {
        showError('Invalid download format.');
        return;
    }
    
    // Create and trigger download
    const blob = new Blob([downloadData], { type: mimeType });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    
    const baseFilename = filename.replace(/\.(csv|json)$/i, '');
    a.href = url;
    a.download = `ai_report_${baseFilename}_${new Date().toISOString().slice(0, 10)}.${fileExtension}`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
    
    showMessage(`Report downloaded as ${a.download}`, 'success');
}

function generateHTMLReport(report, filename) {
    const fileType = filename.toLowerCase().endsWith('.json') ? 'JSON' : 'CSV';
    
    return `
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>AI Data Analysis Report - ${filename}</title>
    <style>
        body {
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
            line-height: 1.6;
            color: #333;
            background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
            margin: 0;
            padding: 2rem;
        }
        .report-container {
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            border-radius: 15px;
            padding: 2rem;
            box-shadow: 0 20px 25px -5px rgba(0, 0, 0, 0.1);
        }
        .report-header {
            text-align: center;
            margin-bottom: 2rem;
            padding: 1.5rem;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            border-radius: 12px;
            color: white;
        }
        .report-section {
            margin-bottom: 2rem;
            background: #f8f9fa;
            border-radius: 12px;
            padding: 1.5rem;
            border-left: 4px solid #667eea;
        }
        .metrics-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 1rem;
            margin-bottom: 1.5rem;
        }
        .metric {
            background: white;
            padding: 1rem;
            border-radius: 8px;
            text-align: center;
            border: 1px solid #e2e8f0;
        }
        .insight-item, .recommendation-item, .step-item {
            background: white;
            padding: 1rem;
            border-radius: 8px;
            margin-bottom: 0.8rem;
            border-left: 4px solid #805ad5;
        }
        .recommendation-item { border-left-color: #38b2ac; }
        .step-item { border-left-color: #ed8936; }
        h1, h2, h3, h4 { color: #4a5568; }
    </style>
</head>
<body>
    <div class="report-container">
        <div class="report-header">
            <h1>🤖 AI Data Analysis Report</h1>
            <p><strong>Dataset:</strong> ${filename} (${fileType})</p>
            <p><strong>Generated:</strong> ${new Date(report.file_info.analysis_timestamp).toLocaleString()}</p>
            <p><strong>Size:</strong> ${report.file_info.total_rows.toLocaleString()} rows × ${report.file_info.total_columns} columns</p>
        </div>
        
        <div class="report-section">
            <h2>📊 Executive Summary</h2>
            <div class="metrics-grid">
                <div class="metric">
                    <strong>Data Quality Score:</strong><br>${report.summary.data_quality_score}
                </div>
                <div class="metric">
                    <strong>Data Completeness:</strong><br>${report.file_info.data_density.toFixed(1)}%
                </div>
                <div class="metric">
                    <strong>Business Value:</strong><br>${report.summary.business_value}
                </div>
                <div class="metric">
                    <strong>File Size:</strong><br>${report.file_info.file_size_estimate}
                </div>
            </div>
            <p><strong>Overview:</strong> ${report.summary.data_overview}</p>
        </div>
        
        ${report.data_quality ? `
        <div class="report-section">
            <h2>🔍 Data Quality Analysis</h2>
            <p><strong>Quality Grade:</strong> ${report.data_quality.quality_grade.toFixed(1)}%</p>
            <p><strong>Completeness:</strong> ${report.data_quality.completeness_score.toFixed(1)}%</p>
            <p><strong>Consistency:</strong> ${report.data_quality.consistency_score.toFixed(1)}%</p>
            ${report.data_quality.data_quality_issues.length > 0 ? 
                `<h3>Issues Found:</h3><ul>${report.data_quality.data_quality_issues.map(issue => `<li>${issue}</li>`).join('')}</ul>` : 
                '<p>✅ No significant data quality issues detected.</p>'
            }
        </div>` : ''}
        
        <div class="report-section">
            <h2>🧠 AI-Powered Insights</h2>
            ${report.ai_insights.map(insight => `<div class="insight-item">${insight}</div>`).join('')}
        </div>
        
        <div class="report-section">
            <h2>💡 AI Recommendations</h2>
            ${report.recommendations.map(rec => `<div class="recommendation-item">${rec}</div>`).join('')}
        </div>
        
        <div class="report-section">
            <h2>🚀 Next Steps</h2>
            ${report.summary.next_steps.map(step => `<div class="step-item">${step}</div>`).join('')}
        </div>
        
        <div class="report-section">
            <p><em>Report generated by DataSmith AI on ${new Date().toLocaleString()}</em></p>
        </div>
    </div>
</body>
</html>`;
}

// Log Processing Functions
let selectedLogFile = null;

function handleLogFileSelect(event) {
    console.log('Log file selection event:', event); // Debug log
    
    const files = event.target.files;
    if (files && files.length > 0) {
        selectedLogFile = files[0];
        console.log('Selected log file:', selectedLogFile.name); // Debug log
        
        const allowedExtensions = ['.log', '.txt'];
        const fileExt = '.' + selectedLogFile.name.toLowerCase().split('.').pop();
        
        if (!allowedExtensions.includes(fileExt)) {
            showError('Please select a .log or .txt file for log processing.');
            selectedLogFile = null;
            if (elements.processLogFileBtn) elements.processLogFileBtn.disabled = true;
            return;
        }
        
        // Update the upload area display
        updateUploadArea('log-file-upload', `Selected: ${selectedLogFile.name}`);
        
        // Enable the process button
        if (elements.processLogFileBtn) {
            elements.processLogFileBtn.disabled = false;
            console.log('Process log file button enabled'); // Debug log
        }
    }
}

async function testLogsAPI() {
    try {
        console.log('Testing logs API connection...');
        const response = await fetch(`${API_BASE_URL}/logs/test`);
        const result = await response.json();
        console.log('API test result:', result);
        return response.ok;
    } catch (error) {
        console.error('API test failed:', error);
        return false;
    }
}

async function processLogFile() {
    console.log('Processing log file, selectedLogFile:', selectedLogFile); // Debug log
    
    if (!selectedLogFile) {
        showError('Please select a log file first.');
        return;
    }

    // Test API connection first
    const apiWorking = await testLogsAPI();
    if (!apiWorking) {
        showError('Cannot connect to backend API. Please make sure the backend server is running.');
        return;
    }

    showLoading('Processing log file through AI pipeline...');
    
    try {
        console.log('Starting log file processing for:', selectedLogFile.name);
        
        const formData = new FormData();
        formData.append('file', selectedLogFile);
        
        console.log('Sending request to:', `${API_BASE_URL}/logs/process-raw`);
        
        // Add timeout to the request
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 60000); // 60 second timeout
        
        const response = await fetch(`${API_BASE_URL}/logs/process-raw`, {
            method: 'POST',
            body: formData,
            signal: controller.signal
        });
        
        clearTimeout(timeoutId);
        
        console.log('Response status:', response.status);
        console.log('Response headers:', response.headers);
        
        let result;
        try {
            result = await response.json();
            console.log('Response data:', result);
        } catch (jsonError) {
            console.error('Failed to parse JSON response:', jsonError);
            const textResponse = await response.text();
            console.error('Raw response:', textResponse);
            throw new Error(`Server returned invalid JSON. Status: ${response.status}. Response: ${textResponse.substring(0, 200)}`);
        }
        
        if (!response.ok) {
            console.error('API error:', result);
            throw new Error(result.detail || result.message || `HTTP ${response.status}: Failed to process log file`);
        }
        
        console.log('Processing successful, displaying results');
        hideLoading();
        displayLogProcessingResults(result, selectedLogFile.name);
        
    } catch (error) {
        console.error('Log processing error:', error);
        hideLoading();
        showError('Log file processing failed: ' + error.message);
    }
}

async function processLogText() {
    const logText = elements.logTextInput?.value?.trim();
    
    if (!logText) {
        showError('Please enter some log text to process.');
        return;
    }

    showLoading('Processing log text through AI pipeline...');
    
    try {
        const response = await fetch(`${API_BASE_URL}/logs/process-text`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ log_text: logText })
        });
        
        const result = await response.json();
        
        if (!response.ok) {
            throw new Error(result.detail || 'Failed to process log text');
        }
        
        hideLoading();
        displayLogProcessingResults(result, 'Log Text Input');
        
    } catch (error) {
        hideLoading();
        showError('Log text processing failed: ' + error.message);
    }
}

function displayLogProcessingResults(result, source) {
    const summary = result.summary || {};
    const anomalyInfo = summary.anomaly_summary || {};
    const parsingInfo = summary.parsing_summary || {};
    
    const resultsHtml = `
        <div class="log-results">
            <div class="log-results-header">
                <h3>🔄 Log Processing Complete</h3>
                <p><strong>Source:</strong> ${source}</p>
                <p><strong>Processed:</strong> ${new Date().toLocaleString()}</p>
            </div>
            
            <div class="log-results-section">
                <h4>📊 Processing Statistics</h4>
                <div class="log-stats-grid">
                    <div class="log-stat">
                        <div class="log-stat-value">${result.total_logs || 0}</div>
                        <div class="log-stat-label">Total Logs</div>
                    </div>
                    <div class="log-stat">
                        <div class="log-stat-value">${result.anomalies_detected || 0}</div>
                        <div class="log-stat-label">Anomalies</div>
                    </div>
                    <div class="log-stat">
                        <div class="log-stat-value">${result.parsing_success_rate || parsingInfo.parsing_success_rate || 0}%</div>
                        <div class="log-stat-label">Parsing Success</div>
                    </div>
                    <div class="log-stat">
                        <div class="log-stat-value">${Object.keys(parsingInfo.format_distribution || {}).length}</div>
                        <div class="log-stat-label">Log Formats</div>
                    </div>
                </div>
                
                ${result.anomalies_detected > 0 ? `
                <div class="anomaly-highlight">
                    ⚠️ ${result.anomalies_detected} anomalies detected - Review immediately!
                </div>
                ` : ''}
            </div>
            
            ${result.ai_insights ? `
            <div class="log-results-section">
                <div class="insights-box">
                    <h4>🧠 AI-Powered Insights</h4>
                    <div class="insights-content">
                        ${result.ai_insights.replace(/\n/g, '<br>')}
                    </div>
                </div>
            </div>
            ` : ''}
            
            ${result.recommendations && result.recommendations.length > 0 ? `
            <div class="log-results-section">
                <h4>💡 Recommendations</h4>
                <ul class="recommendations-list">
                    ${result.recommendations.map(rec => `<li>${rec}</li>`).join('')}
                </ul>
            </div>
            ` : ''}
            
            <div class="log-results-section">
                <h4>📁 Output Files</h4>
                <p><strong>Processed CSV:</strong> ${result.processed_csv || 'Generated'}</p>
                ${result.processed_data ? `
                <p><strong>Sample Data:</strong> Showing first few processed log entries</p>
                <div class="table-container">
                    <table>
                        <thead>
                            <tr>
                                <th>Timestamp</th>
                                <th>Level</th>
                                <th>Message</th>
                                <th>AI Type</th>
                                <th>Severity</th>
                                <th>Anomaly</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${result.processed_data.slice(0, 10).map(log => `
                                <tr>
                                    <td>${log.timestamp_standard || log.timestamp || '-'}</td>
                                    <td>${log.level || '-'}</td>
                                    <td>${(log.message || '').substring(0, 50)}${log.message && log.message.length > 50 ? '...' : ''}</td>
                                    <td>${log.ai_log_type || '-'}</td>
                                    <td>${log.ai_severity || '-'}</td>
                                    <td>${log.is_anomaly ? '⚠️' : '✅'}</td>
                                </tr>
                            `).join('')}
                        </tbody>
                    </table>
                </div>
                ` : ''}
            </div>
        </div>
    `;
    
    showResults({
        title: '🔄 AI Log Processing Results',
        message: 'Log processing completed successfully!',
        data: resultsHtml,
        isHtml: true
    });
};

// ===== PDF PROCESSING FUNCTIONS =====

// PDF File Handlers
function handlePdfCsvFileSelect(file) {
    if (file && file.type === 'application/pdf') {
        currentFiles.pdfCsv = file;
        updateUploadArea('pdf-csv-upload', file.name, 'PDF file ready for CSV conversion');
        elements.processPdfCsvBtn.disabled = false;
    } else {
        showError('Please select a valid PDF file');
    }
}

function handlePdfJsonFileSelect(file) {
    if (file && file.type === 'application/pdf') {
        currentFiles.pdfJson = file;
        updateUploadArea('pdf-json-upload', file.name, 'PDF file ready for JSON conversion');
        elements.processPdfJsonBtn.disabled = false;
    } else {
        showError('Please select a valid PDF file');
    }
}

// PDF to CSV Processing
async function processPdfToCsv() {
    const file = currentFiles.pdfCsv;
    if (!file) {
        showError('Please select a PDF file first');
        return;
    }
    
    showLoading('Extracting data from PDF and converting to CSV...');
    
    try {
        const formData = new FormData();
        formData.append('file', file);
        
        const response = await fetch(`${API_BASE_URL}/pdf/process-to-dataset`, {
            method: 'POST',
            body: formData
        });
        
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const result = await response.json();
        hideLoading();
        
        if (result.success) {
            displayPdfResults(result, 'CSV');
        } else {
            showError(`PDF processing failed: ${result.error || 'Unknown error'}`);
        }
        
    } catch (error) {
        hideLoading();
        console.error('PDF to CSV conversion error:', error);
        showError(`Failed to process PDF: ${error.message}`);
    }
}

// PDF to JSON Processing
async function processPdfToJson() {
    const file = currentFiles.pdfJson;
    if (!file) {
        showError('Please select a PDF file first');
        return;
    }
    
    showLoading('Extracting data from PDF and converting to JSON...');
    
    try {
        const formData = new FormData();
        formData.append('file', file);
        
        const response = await fetch(`${API_BASE_URL}/pdf/process-to-json`, {
            method: 'POST',
            body: formData
        });
        
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const result = await response.json();
        hideLoading();
        
        if (result.success) {
            displayPdfResults(result, 'JSON');
        } else {
            showError(`PDF processing failed: ${result.error || 'Unknown error'}`);
        }
        
    } catch (error) {
        hideLoading();
        console.error('PDF to JSON conversion error:', error);
        showError(`Failed to process PDF: ${error.message}`);
    }
}

// PDF Health Check
async function checkPdfHealthStatus() {
    showLoading('Checking PDF processing dependencies...');
    
    try {
        const response = await fetch(`${API_BASE_URL}/pdf/health-check`);
        const result = await response.json();
        hideLoading();
        
        const statusColor = result.status === 'healthy' ? '#4CAF50' : 
                           result.status === 'missing_dependencies' ? '#FF9800' : '#F44336';
        
        const featuresHtml = result.features ? 
            result.features.map(feature => `<li>${feature}</li>`).join('') : '';
        
        const resultsHtml = `
            <div class="health-check-results">
                <div class="health-status" style="color: ${statusColor};">
                    <h3>
                        <i class="fas fa-heartbeat"></i>
                        PDF Processing Status: ${result.status.toUpperCase()}
                    </h3>
                </div>
                
                <div class="health-details">
                    <h4>📚 PDF Libraries Status</h4>
                    <p><strong>Available:</strong> ${result.pdf_libraries_available ? '✅ Yes' : '❌ No'}</p>
                    
                    ${!result.pdf_libraries_available ? `
                        <div class="installation-guide">
                            <h4>📦 Required Packages</h4>
                            <ul>
                                ${result.required_packages?.map(pkg => `<li>${pkg}</li>`).join('') || ''}
                            </ul>
                            
                            <h4>🛠️ Installation Command</h4>
                            <code class="install-command">${result.install_command || 'pip install pdfplumber PyMuPDF'}</code>
                        </div>
                    ` : ''}
                    
                    ${featuresHtml ? `
                        <h4>🚀 Available Features</h4>
                        <ul class="features-list">
                            ${featuresHtml}
                        </ul>
                    ` : ''}
                </div>
            </div>
        `;
        
        showResults({
            title: '🏥 PDF Processing Health Check',
            message: result.status === 'healthy' ? 
                'All dependencies are properly installed!' : 
                'Some dependencies are missing. Please install them to use PDF processing.',
            data: resultsHtml,
            isHtml: true
        });
        
    } catch (error) {
        hideLoading();
        console.error('Health check error:', error);
        showError(`Failed to check PDF processing status: ${error.message}`);
    }
}

// Display PDF Processing Results
function displayPdfResults(result, format) {
    const sampleData = result.dataframe_info?.sample_data || [];
    const metadata = result.metadata || {};
    const analytics = result.analytics || {};
    
    // Create sample data table
    let sampleDataHtml = '';
    if (sampleData.length > 0) {
        const columns = Object.keys(sampleData[0]);
        sampleDataHtml = `
            <div class="table-container">
                <table>
                    <thead>
                        <tr>
                            ${columns.map(col => `<th>${col}</th>`).join('')}
                        </tr>
                    </thead>
                    <tbody>
                        ${sampleData.map(row => `
                            <tr>
                                ${columns.map(col => `<td>${String(row[col] || '').substring(0, 50)}${String(row[col] || '').length > 50 ? '...' : ''}</td>`).join('')}
                            </tr>
                        `).join('')}
                    </tbody>
                </table>
            </div>
        `;
    }
    
    // Create analytics section
    let analyticsHtml = '';
    if (analytics.data_overview) {
        analyticsHtml = `
            <div class="analytics-section">
                <h4>📊 Data Overview</h4>
                <div class="analytics-grid">
                    <div class="analytics-item">
                        <strong>Total Records:</strong> ${analytics.data_overview.total_records || 0}
                    </div>
                    <div class="analytics-item">
                        <strong>Content Types:</strong> ${Object.keys(analytics.data_overview.content_types || {}).length}
                    </div>
                    <div class="analytics-item">
                        <strong>Business Contexts:</strong> ${Object.keys(analytics.data_overview.business_contexts || {}).length}
                    </div>
                    <div class="analytics-item">
                        <strong>High Quality Records:</strong> ${analytics.data_overview.quality_distribution?.high_quality || 0}
                    </div>
                </div>
                
                ${analytics.insights && analytics.insights.length > 0 ? `
                    <h4>💡 Key Insights</h4>
                    <ul class="insights-list">
                        ${analytics.insights.map(insight => `<li>${insight}</li>`).join('')}
                    </ul>
                ` : ''}
                
                ${analytics.anomalies && analytics.anomalies.length > 0 ? `
                    <h4>⚠️ Anomalies Detected</h4>
                    <ul class="anomalies-list">
                        ${analytics.anomalies.map(anomaly => `<li>${anomaly}</li>`).join('')}
                    </ul>
                ` : ''}
            </div>
        `;
    }
    
    // Create download section
    const downloadHtml = result.output ? `
        <div class="download-section">
            <h4>💾 Download Dataset</h4>
            <div class="download-info">
                <p><strong>Filename:</strong> ${result.output.filename}</p>
                <p><strong>Format:</strong> ${format}</p>
                <p><strong>Size:</strong> ${(result.output.size_bytes / 1024).toFixed(2)} KB</p>
            </div>
            <button class="btn btn-primary" onclick="downloadPdfDataset('${result.output.filename}')">
                <i class="fas fa-download"></i>
                Download ${format} Dataset
            </button>
        </div>
    ` : '';
    
    const resultsHtml = `
        <div class="pdf-results">
            <div class="pdf-processing-summary">
                <h3>📄 PDF Processing Complete</h3>
                <div class="processing-steps">
                    <h4>✅ Processing Steps Completed</h4>
                    <ul class="steps-list">
                        <li>📄 PDF text extraction using ${metadata.extraction_method || 'auto-detection'}</li>
                        <li>🧹 Data cleaning and structuring</li>
                        <li>🧠 AI-powered labeling and classification</li>
                        <li>📊 DataFrame creation (${result.dataframe_info?.shape?.[0] || 0} rows × ${result.dataframe_info?.shape?.[1] || 0} columns)</li>
                        <li>🧮 Analytics and anomaly detection</li>
                        <li>💾 ${format} dataset generation</li>
                    </ul>
                </div>
                
                <div class="metadata-section">
                    <h4>📋 Processing Metadata</h4>
                    <div class="metadata-grid">
                        <div class="metadata-item">
                            <strong>Pages Processed:</strong> ${metadata.total_pages || 0}
                        </div>
                        <div class="metadata-item">
                            <strong>Processing Time:</strong> ${metadata.processing_time ? `${metadata.processing_time.toFixed(2)}s` : 'N/A'}
                        </div>
                        <div class="metadata-item">
                            <strong>Extraction Method:</strong> ${metadata.extraction_method || 'auto'}
                        </div>
                        <div class="metadata-item">
                            <strong>Has Tables:</strong> ${metadata.has_tables ? '✅' : '❌'}
                        </div>
                    </div>
                </div>
            </div>
            
            ${analyticsHtml}
            
            ${sampleData.length > 0 ? `
                <div class="sample-data-section">
                    <h4>📋 Sample Dataset Preview</h4>
                    <p>Showing first ${sampleData.length} records from the generated dataset:</p>
                    ${sampleDataHtml}
                </div>
            ` : ''}
            
            ${downloadHtml}
        </div>
    `;
    
    showResults({
        title: `🔄 PDF to ${format} Conversion Results`,
        message: `Successfully extracted and converted PDF to ${format} dataset!`,
        data: resultsHtml,
        isHtml: true
    });
}

// Download PDF Dataset
async function downloadPdfDataset(filename) {
    try {
        const response = await fetch(`${API_BASE_URL}/pdf/download/${filename}`);
        
        if (!response.ok) {
            throw new Error(`Download failed: ${response.status}`);
        }
        
        const blob = await response.blob();
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        window.URL.revokeObjectURL(url);
        document.body.removeChild(a);
        
        showSuccess(`Successfully downloaded ${filename}`);
    } catch (error) {
        console.error('Download error:', error);
        showError(error.message);
    }
}

// ===== PDF PROCESSING FUNCTIONS =====

// File selection handlers for PDF processing
function handlePdfCsvFileSelect(event) {
    const file = event.target.files[0];
    if (file) {
        console.log('PDF file selected for CSV conversion:', file.name);
        currentFiles.pdfCsv = file;
        updateUploadArea('pdf-csv-upload', file);
        elements.processPdfCsvBtn.disabled = false;
    }
}

function handlePdfJsonFileSelect(event) {
    const file = event.target.files[0];
    if (file) {
        console.log('PDF file selected for JSON conversion:', file.name);
        currentFiles.pdfJson = file;
        updateUploadArea('pdf-json-upload', file);
        elements.processPdfJsonBtn.disabled = false;
    }
}

// PDF to CSV conversion
async function processPdfToCsv() {
    const file = currentFiles.pdfCsv;
    if (!file) {
        showError('Please select a PDF file first');
        return;
    }
    
    if (!file.name.toLowerCase().endsWith('.pdf')) {
        showError('Please select a valid PDF file');
        return;
    }
    
    try {
        showLoading('Converting PDF to CSV dataset...');
        
        const formData = new FormData();
        formData.append('file', file);
        
        const response = await fetch(`${API_BASE_URL}/pdf/process-to-dataset`, {
            method: 'POST',
            body: formData
        });
        
        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.detail || 'PDF processing failed');
        }
        
        const result = await response.json();
        hideLoading();
        
        console.log('PDF to CSV processing result:', result);
        
        if (result.success) {
            displayPdfProcessingResults(result, 'CSV');
            showSuccess(`Successfully converted ${file.name} to CSV dataset`);
        } else {
            throw new Error(result.error || 'PDF processing failed');
        }
        
    } catch (error) {
        hideLoading();
        console.error('PDF to CSV processing error:', error);
        showError(`PDF processing failed: ${error.message}`);
    }
}

// PDF to JSON conversion
async function processPdfToJson() {
    const file = currentFiles.pdfJson;
    if (!file) {
        showError('Please select a PDF file first');
        return;
    }
    
    if (!file.name.toLowerCase().endsWith('.pdf')) {
        showError('Please select a valid PDF file');
        return;
    }
    
    try {
        showLoading('Converting PDF to JSON dataset...');
        
        const formData = new FormData();
        formData.append('file', file);
        
        const response = await fetch(`${API_BASE_URL}/pdf/process-to-json`, {
            method: 'POST',
            body: formData
        });
        
        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.detail || 'PDF processing failed');
        }
        
        const result = await response.json();
        hideLoading();
        
        console.log('PDF to JSON processing result:', result);
        
        if (result.success) {
            displayPdfProcessingResults(result, 'JSON');
            showSuccess(`Successfully converted ${file.name} to JSON dataset`);
        } else {
            throw new Error(result.error || 'PDF processing failed');
        }
        
    } catch (error) {
        hideLoading();
        console.error('PDF to JSON processing error:', error);
        showError(`PDF processing failed: ${error.message}`);
    }
}

// PDF health check
async function checkPdfHealth() {
    try {
        showLoading('Checking PDF processing dependencies...');
        
        const response = await fetch(`${API_BASE_URL}/pdf/health-check`);
        const result = await response.json();
        
        hideLoading();
        
        console.log('PDF health check result:', result);
        displayPdfHealthResults(result);
        
    } catch (error) {
        hideLoading();
        console.error('PDF health check error:', error);
        showError(`Health check failed: ${error.message}`);
    }
}

// Display PDF processing results
function displayPdfProcessingResults(result, format) {
    const resultsHtml = `
        <div class="result-card">
            <div class="result-header">
                <i class="fas fa-file-pdf"></i>
                <h3>PDF to ${format} Conversion Results</h3>
                <div class="result-actions">
                    <button class="btn btn-primary" onclick="downloadPdfDataset('${result.output.filename}')">
                        <i class="fas fa-download"></i>
                        Download ${format}
                    </button>
                </div>
            </div>
            
            <div class="result-content">
                <div class="stats-grid">
                    <div class="stat-item">
                        <span class="stat-label">Processing Steps</span>
                        <span class="stat-value">${Object.keys(result.processing_steps).length}</span>
                    </div>
                    <div class="stat-item">
                        <span class="stat-label">Total Rows</span>
                        <span class="stat-value">${result.metadata.total_rows}</span>
                    </div>
                    <div class="stat-item">
                        <span class="stat-label">Total Columns</span>
                        <span class="stat-value">${result.metadata.total_columns}</span>
                    </div>
                    <div class="stat-item">
                        <span class="stat-label">Processing Time</span>
                        <span class="stat-value">${result.metadata.processing_time.toFixed(2)}s</span>
                    </div>
                    <div class="stat-item">
                        <span class="stat-label">Extraction Method</span>
                        <span class="stat-value">${result.metadata.extraction_method}</span>
                    </div>
                    <div class="stat-item">
                        <span class="stat-label">Output Size</span>
                        <span class="stat-value">${(result.output.size_bytes / 1024).toFixed(1)} KB</span>
                    </div>
                </div>
                
                <div class="processing-steps">
                    <h4><i class="fas fa-cogs"></i> Processing Steps</h4>
                    <div class="steps-list">
                        ${Object.entries(result.processing_steps).map(([step, status]) => 
                            `<div class="step-item ${status === 'completed' ? 'completed' : 'pending'}">
                                <i class="fas fa-${status === 'completed' ? 'check' : 'clock'}"></i>
                                <span>${step.replace(/_/g, ' ').toUpperCase()}</span>
                            </div>`
                        ).join('')}
                    </div>
                </div>
                
                ${result.dataframe_info.sample_data.length > 0 ? `
                <div class="preview-section">
                    <h4><i class="fas fa-eye"></i> Data Preview</h4>
                    <div class="table-container">
                        <table class="data-table">
                            <thead>
                                <tr>
                                    ${result.dataframe_info.columns.map(col => `<th>${col}</th>`).join('')}
                                </tr>
                            </thead>
                            <tbody>
                                ${result.dataframe_info.sample_data.slice(0, 5).map(row => 
                                    `<tr>
                                        ${result.dataframe_info.columns.map(col => 
                                            `<td>${row[col] !== null && row[col] !== undefined ? row[col] : ''}</td>`
                                        ).join('')}
                                    </tr>`
                                ).join('')}
                            </tbody>
                        </table>
                    </div>
                </div>
                ` : ''}
                
                ${result.analytics && !result.analytics.error ? `
                <div class="analytics-section">
                    <h4><i class="fas fa-chart-line"></i> Analytics</h4>
                    <div class="analytics-grid">
                        <div class="analytics-item">
                            <h5>Content Types</h5>
                            <ul>
                                ${Object.entries(result.analytics.data_overview.content_types).map(([type, count]) => 
                                    `<li>${type}: ${count}</li>`
                                ).join('')}
                            </ul>
                        </div>
                        <div class="analytics-item">
                            <h5>Quality Distribution</h5>
                            <ul>
                                ${Object.entries(result.analytics.data_overview.quality_distribution).map(([level, count]) => 
                                    `<li>${level.replace(/_/g, ' ')}: ${count}</li>`
                                ).join('')}
                            </ul>
                        </div>
                        ${result.analytics.anomalies.length > 0 ? `
                        <div class="analytics-item">
                            <h5>Anomalies Detected</h5>
                            <ul>
                                ${result.analytics.anomalies.map(anomaly => `<li>${anomaly}</li>`).join('')}
                            </ul>
                        </div>
                        ` : ''}
                        <div class="analytics-item">
                            <h5>AI Insights</h5>
                            <ul>
                                ${result.analytics.insights.map(insight => `<li>${insight}</li>`).join('')}
                            </ul>
                        </div>
                    </div>
                </div>
                ` : ''}
            </div>
        </div>
    `;
    
    elements.resultsContainer.innerHTML = resultsHtml;
    elements.resultsContainer.scrollIntoView({ behavior: 'smooth' });
}

// Display PDF health check results
function displayPdfHealthResults(result) {
    const statusIcon = result.status === 'healthy' ? 'fa-check-circle' : 'fa-exclamation-triangle';
    const statusClass = result.status === 'healthy' ? 'success' : 'warning';
    
    const resultsHtml = `
        <div class="result-card">
            <div class="result-header">
                <i class="fas fa-heartbeat"></i>
                <h3>PDF Processing Health Check</h3>
            </div>
            
            <div class="result-content">
                <div class="health-status ${statusClass}">
                    <i class="fas ${statusIcon}"></i>
                    <span>Status: ${result.status.toUpperCase()}</span>
                </div>
                
                <div class="health-details">
                    <div class="health-item">
                        <strong>PDF Libraries Available:</strong> 
                        <span class="${result.pdf_libraries_available ? 'success' : 'error'}">
                            ${result.pdf_libraries_available ? 'YES' : 'NO'}
                        </span>
                    </div>
                    
                    ${!result.pdf_libraries_available ? `
                    <div class="health-item">
                        <strong>Required Packages:</strong>
                        <ul>
                            ${result.required_packages.map(pkg => `<li>${pkg}</li>`).join('')}
                        </ul>
                    </div>
                    
                    <div class="health-item">
                        <strong>Install Command:</strong>
                        <code>${result.install_command}</code>
                    </div>
                    ` : ''}
                    
                    <div class="health-item">
                        <strong>Available Features:</strong>
                        <ul>
                            ${result.features.map(feature => `<li>${feature}</li>`).join('')}
                        </ul>
                    </div>
                </div>
            </div>
        </div>
    `;
    
    elements.resultsContainer.innerHTML = resultsHtml;
    elements.resultsContainer.scrollIntoView({ behavior: 'smooth' });
    
    if (result.status === 'healthy') {
        showSuccess('PDF processing is ready to use!');
    } else {
        showError('PDF processing dependencies are missing. Please install required packages.');
    }
}

// Download PDF dataset
async function downloadPdfDataset(filename) {
    try {
        showLoading('Preparing download...');
        
        const response = await fetch(`${API_BASE_URL}/pdf/download/${filename}`);
        
        if (!response.ok) {
            throw new Error(`Download failed: ${response.status}`);
        }
        
        const blob = await response.blob();
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        window.URL.revokeObjectURL(url);
        document.body.removeChild(a);
        
        hideLoading();
        showSuccess(`Successfully downloaded ${filename}`);
        
    } catch (error) {
        hideLoading();
        console.error('PDF dataset download error:', error);
        showError(`Download failed: ${error.message}`);
    }
}

// ===== LOG COLUMN SELECTION FUNCTIONS =====

// Global variables for column selection
let currentLogColumns = null;
let currentLogText = null;
let currentLogFilename = null;

// Handle log file selection for column analysis
function handleLogColumnFileSelect(event) {
    const file = event.target.files[0];
    if (file) {
        console.log('Log file selected for column analysis:', file.name);
        currentLogFilename = file.name;
        
        // Read file content
        const reader = new FileReader();
        reader.onload = function(e) {
            currentLogText = e.target.result;
            analyzeLogColumns();
        };
        reader.readAsText(file);
        
        updateUploadArea('log-column-upload', file);
    }
}

// Analyze log columns from uploaded file
async function analyzeLogColumns() {
    if (!currentLogText) {
        showError('No log content to analyze');
        return;
    }
    
    try {
        showLoading('Analyzing log structure and available columns...');
        
        const formData = new FormData();
        const blob = new Blob([currentLogText], { type: 'text/plain' });
        formData.append('file', blob, currentLogFilename || 'log.txt');
        
        const response = await fetch(`${API_BASE_URL}/logs/get-columns`, {
            method: 'POST',
            body: formData
        });
        
        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.detail || 'Failed to analyze log columns');
        }
        
        const result = await response.json();
        hideLoading();
        
        if (result.success) {
            currentLogColumns = result.columns;
            displayColumnSelection(result.columns);
            showSuccess(`Found ${result.columns.total_columns} columns in your log data`);
        } else {
            throw new Error('Failed to analyze log structure');
        }
        
    } catch (error) {
        hideLoading();
        console.error('Log column analysis error:', error);
        showError(`Failed to analyze log columns: ${error.message}`);
    }
}

// Display column selection interface
function displayColumnSelection(columnData) {
    const container = elements.columnSelectionContainer;
    const categoriesContainer = elements.columnCategories;
    
    if (!container || !categoriesContainer) {
        console.error('Column selection containers not found');
        return;
    }
    
    // Show the container
    container.classList.remove('hidden');
    
    // Clear previous content
    categoriesContainer.innerHTML = '';
    
    // Get category icons
    const categoryIcons = {
        'Basic Info': 'fa-info-circle',
        'Timestamp': 'fa-clock',
        'Network': 'fa-network-wired',
        'HTTP': 'fa-globe',
        'AI Analysis': 'fa-brain',
        'Quality Metrics': 'fa-chart-line',
        'System': 'fa-cog',
        'Other': 'fa-ellipsis-h'
    };
    
    // Create category sections
    Object.entries(columnData.column_categories).forEach(([categoryName, columns]) => {
        const categoryDiv = document.createElement('div');
        categoryDiv.className = 'column-category';
        
        const iconClass = categoryIcons[categoryName] || 'fa-list';
        
        categoryDiv.innerHTML = `
            <div class="category-header">
                <i class="fas ${iconClass}"></i>
                <span>${categoryName}</span>
                <span class="category-count">(${columns.length})</span>
            </div>
            <div class="column-list">
                ${columns.map(colName => {
                    const colInfo = columnData.available_columns[colName];
                    const sampleText = colInfo.sample_values.length > 0 ? 
                        colInfo.sample_values.slice(0, 2).join(', ') : 'No samples';
                    
                    return `
                        <div class="column-item">
                            <input type="checkbox" class="column-checkbox" 
                                   data-column="${colName}" id="col_${colName}">
                            <div class="column-info">
                                <div class="column-name">${colName}</div>
                                <div class="column-description">${colInfo.description}</div>
                                <div class="column-meta">
                                    <span><i class="fas fa-database"></i> ${colInfo.data_type}</span>
                                    <span><i class="fas fa-percentage"></i> ${colInfo.null_percentage}% null</span>
                                    <span><i class="fas fa-list-ol"></i> ${colInfo.total_values} values</span>
                                </div>
                                <div class="sample-values">Sample: ${sampleText}</div>
                            </div>
                        </div>
                    `;
                }).join('')}
            </div>
        `;
        
        categoriesContainer.appendChild(categoryDiv);
    });
    
    // Add event listeners to checkboxes
    addColumnCheckboxListeners();
    updateSelectedCount();
}

// Add event listeners to column checkboxes
function addColumnCheckboxListeners() {
    const checkboxes = document.querySelectorAll('.column-checkbox');
    checkboxes.forEach(checkbox => {
        checkbox.addEventListener('change', updateSelectedCount);
    });
}

// Select all columns
function selectAllColumns() {
    const checkboxes = document.querySelectorAll('.column-checkbox');
    checkboxes.forEach(checkbox => {
        checkbox.checked = true;
    });
    updateSelectedCount();
}

// Deselect all columns
function deselectAllColumns() {
    const checkboxes = document.querySelectorAll('.column-checkbox');
    checkboxes.forEach(checkbox => {
        checkbox.checked = false;
    });
    updateSelectedCount();
}

// Select essential columns only
function selectEssentialColumns() {
    const essentialColumns = ['timestamp', 'level', 'message', 'ip_address', 'ai_log_type', 'is_anomaly'];
    
    const checkboxes = document.querySelectorAll('.column-checkbox');
    checkboxes.forEach(checkbox => {
        const columnName = checkbox.getAttribute('data-column');
        checkbox.checked = essentialColumns.includes(columnName);
    });
    updateSelectedCount();
}

// Update selected count display
function updateSelectedCount() {
    const checkboxes = document.querySelectorAll('.column-checkbox:checked');
    const count = checkboxes.length;
    
    if (elements.selectedCount) {
        elements.selectedCount.textContent = `${count} column${count !== 1 ? 's' : ''} selected`;
    }
    
    if (elements.createFilteredCsvBtn) {
        elements.createFilteredCsvBtn.disabled = count === 0;
    }
}

// Create filtered CSV with selected columns
async function createFilteredCsv() {
    const selectedCheckboxes = document.querySelectorAll('.column-checkbox:checked');
    const selectedColumns = Array.from(selectedCheckboxes).map(cb => cb.getAttribute('data-column'));
    
    if (selectedColumns.length === 0) {
        showError('Please select at least one column');
        return;
    }
    
    if (!currentLogText) {
        showError('No log data available. Please upload a log file first.');
        return;
    }
    
    try {
        showLoading(`Creating filtered CSV with ${selectedColumns.length} selected columns...`);
        
        const requestData = {
            selected_columns: selectedColumns,
            log_text: currentLogText,
            original_filename: currentLogFilename || 'logs.txt'
        };
        
        const response = await fetch(`${API_BASE_URL}/logs/create-filtered-csv`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(requestData)
        });
        
        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.detail || 'Failed to create filtered CSV');
        }
        
        const result = await response.json();
        hideLoading();
        
        if (result.success) {
            displayFilteredCsvResults(result);
            showSuccess(`Filtered CSV created with ${selectedColumns.length} columns`);
        } else {
            throw new Error('Failed to create filtered CSV');
        }
        
    } catch (error) {
        hideLoading();
        console.error('Filtered CSV creation error:', error);
        showError(`Failed to create filtered CSV: ${error.message}`);
    }
}

// Display filtered CSV results
function displayFilteredCsvResults(result) {
    const resultsHtml = `
        <div class="result-card">
            <div class="result-header">
                <i class="fas fa-table"></i>
                <h3>Filtered Log Dataset</h3>
                <div class="result-actions">
                    <button class="btn btn-primary" onclick="downloadFilteredCsv('${result.filename}')">
                        <i class="fas fa-download"></i>
                        Download CSV
                    </button>
                </div>
            </div>
            
            <div class="result-content">
                <div class="stats-grid">
                    <div class="stat-item">
                        <span class="stat-label">Total Rows</span>
                        <span class="stat-value">${result.total_rows}</span>
                    </div>
                    <div class="stat-item">
                        <span class="stat-label">Selected Columns</span>
                        <span class="stat-value">${result.selected_columns.length}</span>
                    </div>
                    <div class="stat-item">
                        <span class="stat-label">File Size</span>
                        <span class="stat-value">${result.file_size_estimate}</span>
                    </div>
                </div>
                
                <div class="selected-columns-info">
                    <h4><i class="fas fa-columns"></i> Selected Columns</h4>
                    <div class="columns-list">
                        ${result.selected_columns.map(col => `<span class="column-tag">${col}</span>`).join('')}
                    </div>
                </div>
                
                ${result.filtered_data_preview.length > 0 ? `
                <div class="preview-section">
                    <h4><i class="fas fa-eye"></i> Data Preview</h4>
                    <div class="table-container">
                        <table class="data-table">
                            <thead>
                                <tr>
                                    ${result.selected_columns.map(col => `<th>${col}</th>`).join('')}
                                </tr>
                            </thead>
                            <tbody>
                                ${result.filtered_data_preview.slice(0, 5).map(row => 
                                    `<tr>
                                        ${result.selected_columns.map(col => 
                                            `<td>${row[col] !== null && row[col] !== undefined ? row[col] : ''}</td>`
                                        ).join('')}
                                    </tr>`
                                ).join('')}
                            </tbody>
                        </table>
                    </div>
                </div>
                ` : ''}
            </div>
        </div>
    `;
    
    elements.resultsContainer.innerHTML = resultsHtml;
    elements.resultsContainer.scrollIntoView({ behavior: 'smooth' });
}

// Download filtered CSV
async function downloadFilteredCsv(filename) {
    try {
        showLoading('Preparing download...');
        
        const response = await fetch(`${API_BASE_URL}/logs/download-filtered/${filename}`);
        
        if (!response.ok) {
            throw new Error(`Download failed: ${response.status}`);
        }
        
        const blob = await response.blob();
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        window.URL.revokeObjectURL(url);
        document.body.removeChild(a);
        
        hideLoading();
        showSuccess(`Successfully downloaded ${filename}`);
        
    } catch (error) {
        hideLoading();
        console.error('Filtered CSV download error:', error);
        showError(`Download failed: ${error.message}`);
    }
}

// ===== LM STUDIO CONNECTION FUNCTIONS =====

// Global variables for LM Studio connection
let currentConnection = {
    url: 'http://localhost:1234',
    connected: false,
    availableModels: [],
    selectedModel: null
};

// Initialize LM Studio connection on page load
function initializeLMStudioConnection() {
    // Load saved settings from localStorage
    const savedUrl = localStorage.getItem('lm_studio_url');
    const savedModel = localStorage.getItem('lm_studio_model');
    
    if (savedUrl) {
        currentConnection.url = savedUrl;
        if (elements.lmStudioUrl) {
            elements.lmStudioUrl.value = savedUrl;
        }
    }
    
    if (savedModel) {
        currentConnection.selectedModel = savedModel;
    }
    
    // Auto-test connection on page load
    setTimeout(() => {
        testLMStudioConnection(false); // Silent test
    }, 1000);
}

// Test connection to LM Studio
async function testLMStudioConnection(showMessages = true) {
    const url = elements.lmStudioUrl?.value || currentConnection.url;
    
    if (showMessages) {
        updateConnectionStatus('connecting', 'Testing connection...');
        elements.testConnectionBtn.disabled = true;
    }
    
    try {
        // Use backend proxy to test connection
        const response = await fetch(`${API_BASE_URL}/lm-studio/test-connection`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                lm_studio_url: url
            })
        });
        
        const result = await response.json();
        
        if (result.success && result.connected) {
            // Update connection state
            currentConnection.connected = true;
            currentConnection.url = url;
            currentConnection.availableModels = result.models || [];
            
            // Update UI
            updateConnectionStatus('connected', 'Connected');
            populateModelList(currentConnection.availableModels);
            
            // Enable model controls
            elements.modelSelect.disabled = false;
            elements.refreshModelsBtn.disabled = false;
            
            if (showMessages) {
                showSuccess(result.message || `Connected to LM Studio! Found ${currentConnection.availableModels.length} available models.`);
            }
            
            // Try to restore previously selected model
            if (currentConnection.selectedModel) {
                const modelExists = currentConnection.availableModels.find(m => m.id === currentConnection.selectedModel);
                if (modelExists) {
                    elements.modelSelect.value = currentConnection.selectedModel;
                }
            }
        } else {
            // Connection failed
            currentConnection.connected = false;
            currentConnection.availableModels = [];
            
            // Update UI
            updateConnectionStatus('disconnected', 'Connection failed');
            clearModelList();
            
            // Disable model controls
            elements.modelSelect.disabled = true;
            elements.refreshModelsBtn.disabled = true;
            
            if (showMessages) {
                showError(result.message || 'Failed to connect to LM Studio');
            }
        }
        
    } catch (error) {
        // Update connection state
        currentConnection.connected = false;
        currentConnection.availableModels = [];
        
        // Update UI
        updateConnectionStatus('disconnected', 'Connection failed');
        clearModelList();
        
        // Disable model controls
        elements.modelSelect.disabled = true;
        elements.refreshModelsBtn.disabled = true;
        
        if (showMessages) {
            showError(`Failed to connect to LM Studio: ${error.message}`);
        }
        
        console.error('LM Studio connection error:', error);
    } finally {
        if (showMessages) {
            elements.testConnectionBtn.disabled = false;
        }
    }
}

// Update connection status indicator
function updateConnectionStatus(status, text) {
    if (!elements.connectionStatus) return;
    
    const indicator = elements.connectionStatus.querySelector('.status-indicator');
    const statusText = elements.connectionStatus.querySelector('.status-text');
    
    // Remove existing status classes
    indicator.classList.remove('connected', 'connecting');
    
    // Add appropriate class
    if (status === 'connected') {
        indicator.classList.add('connected');
    } else if (status === 'connecting') {
        indicator.classList.add('connecting');
    }
    
    // Update status text
    if (statusText) {
        statusText.textContent = text;
    }
}

// Populate model dropdown with available models
function populateModelList(models) {
    if (!elements.modelSelect) return;
    
    // Clear existing options
    elements.modelSelect.innerHTML = '';
    
    if (models.length === 0) {
        elements.modelSelect.innerHTML = '<option value="">No models available</option>';
        return;
    }
    
    // Add default option
    elements.modelSelect.innerHTML = '<option value="">Select a model...</option>';
    
    // Add model options
    models.forEach(model => {
        const option = document.createElement('option');
        option.value = model.id;
        option.textContent = `${model.id} ${model.object ? `(${model.object})` : ''}`;
        elements.modelSelect.appendChild(option);
    });
}

// Clear model dropdown
function clearModelList() {
    if (!elements.modelSelect) return;
    elements.modelSelect.innerHTML = '<option value="">Connect to see available models...</option>';
}

// Refresh available models
async function refreshAvailableModels() {
    if (!currentConnection.connected) {
        showError('Not connected to LM Studio. Please test the connection first.');
        return;
    }
    
    elements.refreshModelsBtn.disabled = true;
    
    try {
        const response = await fetch(`${API_BASE_URL}/lm-studio/models?lm_studio_url=${encodeURIComponent(currentConnection.url)}`);
        
        if (!response.ok) {
            throw new Error(`Failed to fetch models: ${response.status}`);
        }
        
        const result = await response.json();
        
        if (result.success) {
            currentConnection.availableModels = result.models || [];
            populateModelList(currentConnection.availableModels);
            showSuccess(`Refreshed model list. Found ${currentConnection.availableModels.length} models.`);
        } else {
            throw new Error('Failed to fetch models from LM Studio');
        }
        
    } catch (error) {
        showError(`Failed to refresh models: ${error.message}`);
        console.error('Model refresh error:', error);
    } finally {
        elements.refreshModelsBtn.disabled = false;
    }
}

// Save LM Studio settings to localStorage
function saveLMStudioSettings() {
    if (elements.lmStudioUrl?.value) {
        currentConnection.url = elements.lmStudioUrl.value;
        localStorage.setItem('lm_studio_url', currentConnection.url);
    }
    
    if (elements.modelSelect?.value) {
        currentConnection.selectedModel = elements.modelSelect.value;
        localStorage.setItem('lm_studio_model', currentConnection.selectedModel);
    }
}

// Get current LM Studio configuration for API calls
function getLMStudioConfig() {
    return {
        url: currentConnection.url,
        model: currentConnection.selectedModel,
        connected: currentConnection.connected
    };
}

// Add to initialization
document.addEventListener('DOMContentLoaded', function() {
    // ... existing initialization code ...
    initializeLMStudioConnection();
});