class ExcelUploader {
    constructor() {
        this.files = [];
        this.init();
    }

    init() {
        this.uploadArea = document.getElementById('uploadArea');
        this.fileInput = document.getElementById('fileInput');
        this.fileList = document.getElementById('fileList');
        this.uploadBtn = document.getElementById('uploadBtn');
        this.clearBtn = document.getElementById('clearBtn');
        this.agentBtn = document.getElementById('agentBtn');
        this.status = document.getElementById('status');

        this.bindEvents();
    }

    bindEvents() {
        // Drag and drop
        this.uploadArea.addEventListener('dragover', (e) => this.handleDragOver(e));
        this.uploadArea.addEventListener('dragleave', (e) => this.handleDragLeave(e));
        this.uploadArea.addEventListener('drop', (e) => this.handleDrop(e));

        // File input
        this.fileInput.addEventListener('change', (e) => this.handleFileSelect(e));

        // Upload area click
        this.uploadArea.addEventListener('click', (e) => {
            // Não abrir seletor se clicou no botão de upload ou já tem arquivos sendo enviados
            if (e.target.closest('.upload-btn') || this.uploadBtn.disabled) return;
            this.fileInput.click();
        });

        // Buttons
        this.uploadBtn.addEventListener('click', (e) => {
            e.stopPropagation(); // Previne propagação do evento
            this.uploadFiles();
        });
        this.clearBtn.addEventListener('click', (e) => {
            e.stopPropagation(); // Previne propagação do evento
            this.clearFiles();
        });
        this.agentBtn.addEventListener('click', () => this.runAgent());
    }

    handleDragOver(e) {
        e.preventDefault();
        this.uploadArea.classList.add('dragover');
    }

    handleDragLeave(e) {
        e.preventDefault();
        this.uploadArea.classList.remove('dragover');
    }

    handleDrop(e) {
        e.preventDefault();
        this.uploadArea.classList.remove('dragover');

        const files = Array.from(e.dataTransfer.files).filter(file =>
            file.name.endsWith('.xlsx') || file.name.endsWith('.xls')
        );

        this.addFiles(files);
    }

    handleFileSelect(e) {
        const files = Array.from(e.target.files);
        this.addFiles(files);
    }

    addFiles(newFiles) {
        newFiles.forEach(file => {
            if (!this.files.find(f => f.name === file.name && f.size === file.size)) {
                this.files.push(file);
            }
        });

        this.renderFileList();
        this.updateUploadButton();
        this.hideStatus();
    }

    removeFile(index) {
        this.files.splice(index, 1);
        this.renderFileList();
        this.updateUploadButton();
    }

    renderFileList() {
        if (this.files.length === 0) {
            this.fileList.innerHTML = '';
            return;
        }

        this.fileList.innerHTML = this.files.map((file, index) => `
            <div class="file-item">
                <div class="file-info">
                    <div class="file-icon">📊</div>
                    <div class="file-details">
                        <div class="file-name">${file.name}</div>
                        <div class="file-size">${this.formatFileSize(file.size)}</div>
                    </div>
                </div>
                <button class="remove-file" onclick="uploader.removeFile(${index})">
                    Remover
                </button>
            </div>
        `).join('');
    }

    formatFileSize(bytes) {
        if (bytes === 0) return '0 Bytes';
        const k = 1024;
        const sizes = ['Bytes', 'KB', 'MB', 'GB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    }

    updateUploadButton() {
        this.uploadBtn.disabled = this.files.length === 0;
    }

    clearFiles() {
        this.files = [];
        this.fileInput.value = '';
        this.renderFileList();
        this.updateUploadButton();
        this.hideStatus();
    }

    async uploadFiles() {
        if (this.files.length === 0) return;

        const formData = new FormData();
        this.files.forEach(file => {
            formData.append('files', file);
        });

        this.showStatus('Enviando arquivos e processando...', 'loading');
        this.uploadBtn.disabled = true;

        try {
            const response = await fetch('http://localhost:8000/uploadfiles/', {
                method: 'POST',
                body: formData
            });

            if (response.ok) {
                this.clearFiles();
                this.showStatus('Arquivos enviados e processados com sucesso!', 'success');

            } else {
                throw new Error(`Erro HTTP: ${response.status}`);
            }
        } catch (error) {
            this.showStatus(`Erro ao processar arquivos: ${error.message}`, 'error');
        } finally {
            this.uploadBtn.disabled = false;
            this.updateUploadButton();
        }
    }

    showStatus(message, type) {
        this.status.textContent = message;
        this.status.className = `status ${type}`;
    }

    async runAgent() {
        this.showStatus('Executando agent...', 'loading');
        this.agentBtn.disabled = true;

        try {
            const response = await fetch('http://localhost:8000/agent', {
                method: 'GET'
            });

            if (response.ok) {
                const contentType = response.headers.get('Content-Type');

                if (contentType && contentType.includes('application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')) {
                    const blob = await response.blob();
                    const url = window.URL.createObjectURL(blob);
                    const a = document.createElement('a');

                    const contentDisposition = response.headers.get('Content-Disposition');
                    let filename = 'custos_colaboradores.xlsx';

                    if (contentDisposition) {
                        const filenameMatch = contentDisposition.match(/filename="(.+)"/);
                        if (filenameMatch) {
                            filename = filenameMatch[1];
                        }
                    }

                    a.href = url;
                    a.download = filename;
                    document.body.appendChild(a);
                    a.click();
                    window.URL.revokeObjectURL(url);
                    document.body.removeChild(a);

                    this.showStatus('Agent executado com sucesso! Arquivo baixado.', 'success');

                } else {
                    const result = await response.json();
                    if (result.message) {
                        this.showStatus(result.message, 'error');
                    }
                }

            } else {
                throw new Error(`Erro HTTP: ${response.status}`);
            }
        } catch (error) {
            this.showStatus(`Erro ao executar agent: ${error.message}`, 'error');
        } finally {
            this.agentBtn.disabled = false;
        }
    }

    hideStatus() {
        this.status.className = 'status';
    }
}

// Inicializar o uploader quando o DOM estiver carregado
document.addEventListener('DOMContentLoaded', () => {
    window.uploader = new ExcelUploader();
});