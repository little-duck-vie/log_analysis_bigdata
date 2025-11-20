// Global variables
const socket = io();
let detections = [];
let allDetections = [];
let currentLang = 'vi';
let selectMode = false;
let selectedItems = new Set();
let filters = {
    prediction: 'all',
    keyword: '',
    startTime: '',
    endTime: ''
};

// Translations
const translations = {
    vi: {
        pageTitle: 'Bảng điều khiển phát hiện Log',
        pageSubtitle: 'Giám sát thời gian thực',
        streamTitle: 'Dòng phát hiện',
        selectBtn: 'Chọn',
        cancelBtn: 'Hủy',
        deleteBtn: 'Xóa',
        filterLabel: 'Dự đoán',
        searchLabel: 'Tìm kiếm',
        searchPlaceholder: 'Tìm kiếm từ khóa...',
        applyBtn: 'Áp dụng',
        resetBtn: 'Thiết lập lại',
        allChip: 'Tất cả',
        normalChip: 'Bình thường',
        errorChip: 'Lỗi',
        predictionLabel: 'Dự đoán',
        startTimeLabel: 'Thời gian bắt đầu',
        endTimeLabel: 'Thời gian kết thúc',
        durationLabel: 'Thời lượng',
        linesLabel: 'Số dòng',
        timestampLabel: 'Thời gian ghi',
        tsMsLabel: 'TS (ms)',
        featuresLabel: 'Đặc trưng',
        logContentTitle: 'Nội dung Log',
        linesCount: 'dòng',
        noData: 'Không có dữ liệu',
        waitingData: 'Đang chờ dữ liệu...',
        connected: 'Đã kết nối',
        disconnected: 'Mất kết nối',
        connecting: 'Đang kết nối...',
        normalBadge: 'Bình thường',
        errorBadge: 'Lỗi',
        noLogsSelected: 'Chưa chọn log nào!',
        confirmDelete: 'Xóa {count} log đã chọn?\n\nHành động này không thể hoàn tác!',
        deleteSuccess: 'Đã xóa thành công {successCount}/{count} log từ HBase',
        deleteFailed: 'Xóa log thất bại'
    },
    en: {
        pageTitle: 'Log Detection Dashboard',
        pageSubtitle: 'Real-time monitoring',
        streamTitle: 'Detection Stream',
        selectBtn: 'Select',
        cancelBtn: 'Cancel',
        deleteBtn: 'Delete',
        filterLabel: 'Prediction',
        searchLabel: 'Search',
        searchPlaceholder: 'Search keyword...',
        applyBtn: 'Apply',
        resetBtn: 'Reset',
        allChip: 'All',
        normalChip: 'Normal',
        errorChip: 'Error',
        predictionLabel: 'Prediction',
        startTimeLabel: 'Start Time',
        endTimeLabel: 'End Time',
        durationLabel: 'Duration',
        linesLabel: 'Lines',
        timestampLabel: 'Timestamp',
        tsMsLabel: 'TS (ms)',
        featuresLabel: 'Features',
        logContentTitle: 'Log Content',
        linesCount: 'lines',
        noData: 'No data available',
        waitingData: 'Waiting for data...',
        connected: 'Connected',
        disconnected: 'Disconnected',
        connecting: 'Connecting...',
        normalBadge: 'Normal',
        errorBadge: 'Error',
        noLogsSelected: 'No logs selected!',
        confirmDelete: 'Delete {count} selected logs?\n\nThis action cannot be undone!',
        deleteSuccess: 'Successfully deleted {successCount}/{count} logs from HBase',
        deleteFailed: 'Failed to delete logs'
    }
};

function t(key) {
    return translations[currentLang][key] || key;
}

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    setupSocketListeners();
    requestLatest();

    // Auto-refresh every 1 second
    setInterval(() => {
        requestLatest();
    }, 1000);
});

// Socket.IO listeners
function setupSocketListeners() {
    socket.on('connect', () => {
        updateConnectionStatus(true);
        requestLatest();
    });

    socket.on('disconnect', () => {
        updateConnectionStatus(false);
    });

    socket.on('latest_detections', (data) => {
        allDetections = data.detections || [];
        applyFilters();
        updateStats();
    });
}

// Update connection status
function updateConnectionStatus(connected) {
    const statusDot = document.getElementById('connection-status');
    const statusText = document.getElementById('status-text');

    if (connected) {
        statusDot.classList.add('connected');
        statusText.textContent = t('connected');
    } else {
        statusDot.classList.remove('connected');
        statusText.textContent = t('disconnected');
    }
}

// Request latest data
function requestLatest() {
    socket.emit('request_latest', { limit: 500 });
}

// Apply filters
function applyFilters() {
    const keyword = document.getElementById('search-keyword').value.toLowerCase();
    const startTime = document.getElementById('start-time').value;
    const endTime = document.getElementById('end-time').value;
    
    filters.keyword = keyword;
    filters.startTime = startTime;
    filters.endTime = endTime;

    detections = allDetections.filter(d => {
        const prediction = d.fields && d.fields.prediction !== undefined ? d.fields.prediction : 1;
        const logFull = d.fields && d.fields.log_full ? d.fields.log_full : '';
        const startTs = d.fields && d.fields.start_ts ? d.fields.start_ts : '';

        // Filter by prediction
        if (filters.prediction !== 'all' && String(prediction) !== filters.prediction) {
            return false;
        }

        // Filter by keyword
        if (filters.keyword && !logFull.toLowerCase().includes(filters.keyword)) {
            return false;
        }

        // Filter by time range
        if (filters.startTime && startTs) {
            const logTime = new Date(startTs).getTime();
            const filterStartTime = new Date(filters.startTime).getTime();
            if (logTime < filterStartTime) {
                return false;
            }
        }

        if (filters.endTime && startTs) {
            const logTime = new Date(startTs).getTime();
            const filterEndTime = new Date(filters.endTime).getTime();
            if (logTime > filterEndTime) {
                return false;
            }
        }

        return true;
    });

    renderLogs();
}

// Set filter
function setFilter(type, value) {
    filters[type] = value;

    // Update active chip
    document.querySelectorAll('.chip').forEach(chip => {
        chip.classList.remove('active');
    });
    event.target.classList.add('active');
}

// Reset filters
function resetFilters() {
    filters = {
        prediction: 'all',
        keyword: '',
        startTime: '',
        endTime: ''
    };

    document.getElementById('search-keyword').value = '';
    document.getElementById('start-time').value = '';
    document.getElementById('end-time').value = '';
    
    document.querySelectorAll('.chip').forEach((chip, idx) => {
        chip.classList.remove('active');
        if (idx === 0) chip.classList.add('active');
    });

    applyFilters();
}

// Toggle filter panel
function toggleFilter() {
    const panel = document.getElementById('filter-panel');
    panel.style.display = panel.style.display === 'none' ? 'block' : 'none';
}

// Update stats
function updateStats() {
    // Stats removed - no longer needed
}

// Render logs
function renderLogs() {
    const container = document.getElementById('log-list');

    if (detections.length === 0) {
        container.innerHTML = `
            <div class="empty-state">
                <div class="empty-text">${t('noData')}</div>
            </div>
        `;
        return;
    }

    let html = '';
    detections.forEach((d, idx) => {
                const blockId = d.detection_id || 'N/A';
                const prediction = d.fields && d.fields.prediction !== undefined ? d.fields.prediction : 1;
                const badgeClass = prediction === 1 ? 'badge-success' : 'badge-error';
                const badgeText = prediction === 1 ? t('normalBadge') : t('errorBadge');

                const startTs = d.fields && d.fields.start_ts || '-';
                const endTs = d.fields && d.fields.end_ts || '-';
                const duration = d.fields && d.fields.duration_sec || 0;
                const numLines = d.fields && d.fields.num_lines || 0;
                const features = d.fields && d.fields.features ? d.fields.features : 'N/A';
                const timestamp = d.fields && d.fields.timestamp || '-';
                const tsMsRaw = d.fields && d.fields.ts_ms || '-';

                const logFull = d.fields && d.fields.log_full ? d.fields.log_full : '';

                const isSelected = selectedItems.has(blockId);

                html += `
            <div class="log-item ${isSelected ? 'selected' : ''}" onclick="handleLogClick(event, '${escapeHtml(blockId)}')">
                <input type="checkbox" class="log-checkbox" ${isSelected ? 'checked' : ''} 
                       onchange="toggleSelection('${escapeHtml(blockId)}')">
                
                <div class="log-item-header">
                    <span class="block-id">${escapeHtml(blockId)}</span>
                    <span class="badge ${badgeClass}">${badgeText}</span>
                </div>
                
                <div class="log-item-body">
                    <div class="log-field">
                        <div class="log-field-label">${t('predictionLabel')}</div>
                        <div class="log-field-value">${badgeText}</div>
                    </div>
                    <div class="log-field">
                        <div class="log-field-label">${t('startTimeLabel')}</div>
                        <div class="log-field-value">${escapeHtml(startTs)}</div>
                    </div>
                    <div class="log-field">
                        <div class="log-field-label">${t('endTimeLabel')}</div>
                        <div class="log-field-value">${escapeHtml(endTs)}</div>
                    </div>
                    <div class="log-field">
                        <div class="log-field-label">${t('durationLabel')}</div>
                        <div class="log-field-value">${duration}s</div>
                    </div>
                    <div class="log-field">
                        <div class="log-field-label">${t('linesLabel')}</div>
                        <div class="log-field-value">${numLines}</div>
                    </div>
                    <div class="log-field log-field-full">
                        <div class="log-field-label">${t('featuresLabel')}</div>
                        <div class="log-field-value">${escapeHtml(features)}</div>
                    </div>
                </div>
                
                ${logFull ? `
                <div class="log-content">
                    <div class="log-content-header">
                        <span class="log-content-title">${t('logContentTitle')}</span>
                        <span class="log-line-count">${numLines} ${t('linesCount')}</span>
                    </div>
                    <div class="log-content-scroll">${escapeHtml(logFull)}</div>
                </div>
                ` : ''}
            </div>
        `;
    });

    container.innerHTML = html;
}

// Handle log click
function handleLogClick(event, blockId) {
    if (!selectMode) return;
    if (event.target.classList.contains('log-checkbox')) return;
    
    toggleSelection(blockId);
}

// Toggle select mode
function toggleSelectMode() {
    selectMode = !selectMode;
    const container = document.getElementById('log-list');
    const selectBtn = document.getElementById('select-btn');
    const deleteBtn = document.getElementById('delete-btn');
    
    if (selectMode) {
        container.parentElement.classList.add('select-mode');
        selectBtn.textContent = t('cancelBtn');
        deleteBtn.style.display = 'inline-block';
    } else {
        container.parentElement.classList.remove('select-mode');
        selectBtn.textContent = t('selectBtn');
        deleteBtn.style.display = 'none';
        selectedItems.clear();
        renderLogs();
    }
}

// Toggle selection
function toggleSelection(blockId) {
    if (!selectMode) return;
    
    if (selectedItems.has(blockId)) {
        selectedItems.delete(blockId);
    } else {
        selectedItems.add(blockId);
    }
    
    renderLogs();
    updateDeleteButton();
}

// Update delete button
function updateDeleteButton() {
    const deleteBtn = document.getElementById('delete-btn');
    const count = selectedItems.size;
    
    if (count > 0) {
        deleteBtn.textContent = `${t('deleteBtn')} (${count})`;
    } else {
        deleteBtn.textContent = t('deleteBtn');
    }
}

// Delete selected
function deleteSelected() {
    const count = selectedItems.size;
    
    if (count === 0) {
        alert(t('noLogsSelected'));
        return;
    }

    if (!confirm(t('confirmDelete').replace('{count}', count))) {
        return;
    }

    const promises = Array.from(selectedItems).map(detectionId => {
        return fetch(`/api/delete/${encodeURIComponent(detectionId)}`, {
            method: 'DELETE'
        }).then(response => response.json());
    });

    Promise.all(promises)
        .then(results => {
            const successCount = results.filter(r => r.success).length;
            alert(t('deleteSuccess').replace('{successCount}', successCount).replace('{count}', count));
            selectedItems.clear();
            toggleSelectMode();
            requestLatest();
        })
        .catch(error => {
            console.error('Delete error:', error);
            alert(t('deleteFailed'));
        });
}

// Switch language
function switchLang(lang) {
    currentLang = lang;
    
    document.querySelectorAll('.lang-btn').forEach(btn => {
        btn.classList.remove('active');
    });
    event.target.classList.add('active');
    
    updateUILanguage();
    renderLogs();
}

// Update UI language
function updateUILanguage() {
    document.getElementById('page-title').textContent = t('pageTitle');
    document.getElementById('page-subtitle').textContent = t('pageSubtitle');
    document.querySelector('.log-header h2').textContent = t('streamTitle');
    
    const selectBtn = document.getElementById('select-btn');
    selectBtn.textContent = selectMode ? t('cancelBtn') : t('selectBtn');
    
    const deleteBtn = document.getElementById('delete-btn');
    const count = selectedItems.size;
    deleteBtn.textContent = count > 0 ? `${t('deleteBtn')} (${count})` : t('deleteBtn');
}

// Escape HTML
function escapeHtml(str) {
    return String(str)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}