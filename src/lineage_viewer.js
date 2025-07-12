let lineageData = null;
let selectedTable = null;
let network = null;

// Track the currently selected script in the network view
let selectedNetworkScript = null;

// Mappings for fast lookup
let tableKeyMap = {};
let operationKeyMap = {};

// Build mappings after data load
function buildScriptAwareMappings() {
    tableKeyMap = {};
    operationKeyMap = {};
    if (!lineageData || !lineageData.tables) return;
    // Build tableKeyMap
    for (const [tableName, tableObj] of Object.entries(lineageData.tables)) {
        const scriptName = tableObj.script_name || 'Unknown';
        const key = scriptName + '::' + tableName;
        tableKeyMap[key] = tableObj;
        // Build operationKeyMap for sources
        if (tableObj.source) {
            tableObj.source.forEach(rel => {
                if (rel.operation) {
                    rel.operation.forEach(opIdx => {
                        const opKey = scriptName + '::' + opIdx;
                        operationKeyMap[opKey] = lineageData.bteq_statements[opIdx];
                    });
                }
            });
        }
        // Build operationKeyMap for targets
        if (tableObj.target) {
            tableObj.target.forEach(rel => {
                if (rel.operation) {
                    rel.operation.forEach(opIdx => {
                        const opKey = scriptName + '::' + opIdx;
                        operationKeyMap[opKey] = lineageData.bteq_statements[opIdx];
                    });
                }
            });
        }
    }
}

// Call buildScriptAwareMappings after data load
// (add this call in all data load functions, e.g., after lineageData is set)

// Helper function to get script name and local index for a statement
function getStatementInfo(globalIndex) {
    // Find the script and local index for this operation index
    for (const [tableName, tableObj] of Object.entries(lineageData.tables)) {
        const scriptName = tableObj.script_name || 'Unknown';
        // Check if this table's script has this op index
        if (lineageData.file_groups) {
            for (const [fileName, fileGroup] of Object.entries(lineageData.file_groups)) {
                if (fileGroup.statements.includes(globalIndex) && tableObj.script_name) {
                    const localIndex = fileGroup.statements.indexOf(globalIndex);
                    return {
                        scriptName: scriptName,
                        localIndex: localIndex,
                        displayText: `${scriptName}:${localIndex}`,
                        operationKey: scriptName + '::' + globalIndex
                    };
                }
            }
        } else {
            // Single file mode
            if (tableObj.script_name) {
                return {
                    scriptName: scriptName,
                    localIndex: globalIndex,
                    displayText: `${scriptName}:${globalIndex}`,
                    operationKey: scriptName + '::' + globalIndex
                };
            }
        }
    }
    // Fallback
    return {
        scriptName: 'Unknown',
        localIndex: globalIndex,
        displayText: `Unknown:${globalIndex}`,
        operationKey: 'Unknown::' + globalIndex
    };
}

// Helper function to convert operation indices to script names with local indices
function getOperationDisplayText(operationGroups) {
    // operationGroups: Array of {scriptName: string, indices: number[]}
    if (!operationGroups || operationGroups.length === 0) return '';
    return operationGroups.map(group => `${group.scriptName}:${group.indices.join('|')}`).join(', ');
}

// Check for JSON file path or folder in URL parameters on page load
window.onload = function() {
    const urlParams = new URLSearchParams(window.location.search);
    const jsonPath = urlParams.get('json');
    const folderPath = urlParams.get('folder');
    
    if (jsonPath) {
        loadJsonFromPath(jsonPath);
    } else if (folderPath) {
        // Load all_lineage.txt from the specified folder
        loadAllLineageFilesFromFolder(folderPath);
    } else {
        // Load default all_lineage.txt file and process all JSON files
        loadAllLineageFiles();
    }
    
    // Initialize resize functionality
    initializeResizeHandles();
};

function loadJsonFile() {
    const fileInput = document.getElementById('jsonFile');
    const file = fileInput.files[0];
    
    if (!file) {
        alert('Please select a JSON file');
        return;
    }

    const reader = new FileReader();
    reader.onload = function(e) {
        try {
            lineageData = JSON.parse(e.target.result);
            // Add source file information if not present
            if (!lineageData.source_file) {
                lineageData.source_file = file.name;
            }
            
            // Create file groups for single file to maintain consistency
            const fileName = file.name.replace('_lineage.json', '');
            lineageData.file_groups = {
                [fileName]: {
                    tables: Object.keys(lineageData.tables || {}),
                    statements: Array.from({length: lineageData.bteq_statements?.length || 0}, (_, i) => i)
                }
            };
            
            displaySummary();
            displayTables();
            displayStatements();
            displayNetworkFileGroups();
            document.getElementById('summarySection').style.display = 'block';
            document.getElementById('tabSection').style.display = 'block';
            initializeTableNames();
        } catch (error) {
            showError('Error parsing JSON file: ' + error.message);
        }
    };
    reader.readAsText(file);
}

function loadFolder() {
    const folderInput = document.getElementById('folderInput');
    const files = folderInput.files;
    
    if (files.length === 0) {
        showError('Please select a folder');
        return;
    }

    // Filter for JSON files only
    const jsonFiles = Array.from(files).filter(file => file.name.endsWith('.json'));
    
    if (jsonFiles.length === 0) {
        showError('No JSON files found in the selected folder');
        return;
    }

    console.log(`Found ${jsonFiles.length} JSON files in folder`);

    // Show loading information
    const folderInfo = document.getElementById('folderInfo');
    const folderStatus = document.getElementById('folderStatus');
    const folderProgress = document.getElementById('folderProgress');
    
    folderStatus.textContent = `Found ${jsonFiles.length} JSON files in folder`;
    folderProgress.textContent = `Processing files...`;
    folderInfo.style.display = 'block';

    // Initialize merged data structure with file tracking
    const mergedData = {
        tables: {},
        bteq_statements: [],
        source_file: `${jsonFiles.length}`,
        file_groups: {} // Track which file each item came from
    };

    let processedFiles = 0;
    const totalFiles = jsonFiles.length;

    jsonFiles.forEach((file, index) => {
        const reader = new FileReader();
        reader.onload = function(e) {
            try {
                const data = JSON.parse(e.target.result);
                console.log(`Processing file: ${file.name}`);
                
                // Create file group using script_name from the data
                const scriptName = data.script_name || file.name.replace('_lineage.json', '');
                mergedData.file_groups[scriptName] = {
                    tables: [],
                    statements: []
                };
                
                // Merge tables with file tracking
                if (data.tables) {
                    Object.keys(data.tables).forEach(tableName => {
                        mergedData.tables[tableName] = data.tables[tableName];
                        // Add script_name to the table object
                        mergedData.tables[tableName].script_name = scriptName;
                        mergedData.file_groups[scriptName].tables.push(tableName);
                    });
                }
                
                // Merge BTEQ statements with file tracking
                if (data.bteq_statements) {
                    const startIndex = mergedData.bteq_statements.length;
                    mergedData.bteq_statements.push(...data.bteq_statements);
                    
                    // Track statement indices for this file
                    for (let i = 0; i < data.bteq_statements.length; i++) {
                        mergedData.file_groups[scriptName].statements.push(startIndex + i);
                    }
                }
                
                processedFiles++;
                folderProgress.textContent = `Processed ${processedFiles} of ${totalFiles} files...`;
                
                // When all files are processed, update the display
                if (processedFiles === totalFiles) {
                    console.log(`Successfully merged ${totalFiles} files`);
                    lineageData = mergedData;
                    buildScriptAwareMappings();
                    displaySummary();
                    displayTables();
                    displayStatements();
                    displayNetworkFileGroups();
                    document.getElementById('summarySection').style.display = 'block';
                    document.getElementById('tabSection').style.display = 'block';
                    initializeTableNames();
                    
                    // Hide the folder info
                    folderInfo.style.display = 'none';
                    
                    // Update URL to include the folder path
                    const url = new URL(window.location);
                    const folderPath = folderInput.value.split('/').slice(0, -1).join('/');
                    url.searchParams.set('folder', folderPath);
                    window.history.pushState({}, '', url);
                }
            } catch (error) {
                console.error(`Error parsing JSON file ${file.name}:`, error);
                showError(`Error parsing JSON file ${file.name}: ${error.message}`);
                folderInfo.style.display = 'none';
            }
        };
        reader.readAsText(file);
    });
}

function loadFromUrl() {
    // Create a custom input dialog
    const input = document.createElement('input');
    input.type = 'text';
    input.placeholder = 'Enter path to JSON file or folder (e.g., ../report_lm/ or ./script.json)';
    // Use the current URL parameter as default, or fall back to a default path
    const currentJsonPath = new URLSearchParams(window.location.search).get('json');
    const currentFolderPath = new URLSearchParams(window.location.search).get('folder');
    input.value = currentJsonPath || currentFolderPath || './YourScript_lineage.json';
    input.style.cssText = `
        position: fixed;
        top: 50%;
        left: 50%;
        transform: translate(-50%, -50%);
        z-index: 10000;
        padding: 15px;
        border: 2px solid #007bff;
        border-radius: 8px;
        font-size: 16px;
        width: 400px;
        background: white;
        box-shadow: 0 4px 20px rgba(0,0,0,0.3);
    `;
    
    // Create overlay
    const overlay = document.createElement('div');
    overlay.style.cssText = `
        position: fixed;
        top: 0;
        left: 0;
        width: 100%;
        height: 100%;
        background: rgba(0,0,0,0.5);
        z-index: 9999;
    `;
    
    // Create buttons
    const buttonContainer = document.createElement('div');
    buttonContainer.style.cssText = `
        position: fixed;
        top: calc(50% + 60px);
        left: 50%;
        transform: translateX(-50%);
        z-index: 10000;
        display: flex;
        gap: 10px;
    `;
    
    const loadBtn = document.createElement('button');
    loadBtn.textContent = 'Load';
    loadBtn.style.cssText = `
        padding: 8px 16px;
        background: #007bff;
        color: white;
        border: none;
        border-radius: 4px;
        cursor: pointer;
        font-size: 14px;
    `;
    
    const cancelBtn = document.createElement('button');
    cancelBtn.textContent = 'Cancel';
    cancelBtn.style.cssText = `
        padding: 8px 16px;
        background: #6c757d;
        color: white;
        border: none;
        border-radius: 4px;
        cursor: pointer;
        font-size: 14px;
    `;
    
    // Add event listeners
    const cleanup = () => {
        document.body.removeChild(overlay);
        document.body.removeChild(input);
        document.body.removeChild(buttonContainer);
    };
    
    loadBtn.onclick = () => {
        if (input.value.trim()) {
            loadFromPath(input.value.trim());
        }
        cleanup();
    };
    
    cancelBtn.onclick = cleanup;
    
    input.onkeydown = (e) => {
        if (e.key === 'Enter') {
            if (input.value.trim()) {
                loadFromPath(input.value.trim());
            }
            cleanup();
        } else if (e.key === 'Escape') {
            cleanup();
        }
    };
    
    // Add to DOM
    document.body.appendChild(overlay);
    document.body.appendChild(input);
    document.body.appendChild(buttonContainer);
    buttonContainer.appendChild(loadBtn);
    buttonContainer.appendChild(cancelBtn);
    
    // Focus the input
    input.focus();
    input.select();
}

function loadFromPath(path) {
    const trimmedPath = path.trim();
    
    // Check if it's a folder path (ends with /)
    if (trimmedPath.endsWith('/')) {
        // Load folder - look for all_lineage.txt in the folder
        loadAllLineageFilesFromFolder(trimmedPath);
        return;
    }
    
    // Check if it's a JSON file (ends with .json)
    if (trimmedPath.endsWith('.json')) {
        // Load single JSON file
        loadJsonFromPath(trimmedPath);
        return;
    }
    
    // If neither, assume it's a folder and try to load all_lineage.txt
    // Add trailing slash if not present
    const folderPath = trimmedPath.endsWith('/') ? trimmedPath : trimmedPath + '/';
    loadAllLineageFilesFromFolder(folderPath);
}

function loadJsonFromPath(jsonPath) {
    fetch(jsonPath)
        .then(response => {
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            return response.json();
        })
        .then(data => {
            lineageData = data;
            // Add source file information if not present
            if (!lineageData.source_file) {
                lineageData.source_file = jsonPath;
            }
            
            // Create file groups for single file to maintain consistency
            const fileName = jsonPath.split('/').pop().replace('_lineage.json', '');
            lineageData.file_groups = {
                [fileName]: {
                    tables: Object.keys(lineageData.tables || {}),
                    statements: Array.from({length: lineageData.bteq_statements?.length || 0}, (_, i) => i)
                }
            };
            
            displaySummary();
            displayTables();
            displayStatements();
            displayNetworkFileGroups();
            document.getElementById('summarySection').style.display = 'block';
            document.getElementById('tabSection').style.display = 'block';
            initializeTableNames();
            
            // Update URL to include the JSON path
            const url = new URL(window.location);
            url.searchParams.set('json', jsonPath);
            window.history.pushState({}, '', url);
        })
        .catch(error => {
            showError('Error loading JSON file: ' + error.message);
        });
}

function displaySummary() {
    const summaryGrid = document.getElementById('summaryGrid');
    const tables = lineageData.tables;
    
    const totalTables = Object.keys(tables).length;
    const sourceTables = Object.values(tables).filter(t => t.source.length > 0).length;
    const targetTables = Object.values(tables).filter(t => t.target.length > 0).length;
    const volatileTables = Object.values(tables).filter(t => t.is_volatile).length;
    const totalOperations = lineageData.bteq_statements.length;

    // Determine script count and display name(s)
    let scriptCount = 1;
    let scriptDisplay = '';
    if (lineageData.file_groups && Object.keys(lineageData.file_groups).length > 1) {
        scriptCount = Object.keys(lineageData.file_groups).length;
        scriptDisplay = '';
    } else if (lineageData.file_groups && Object.keys(lineageData.file_groups).length === 1) {
        // Single file mode, show cleaned-up script name
        let src = Object.keys(lineageData.file_groups)[0];
        // Remove path if present
        src = src.split('/').pop();
        // Remove .json extension
        src = src.replace(/\.json$/i, '');
        // Convert *_sh_lineage or *_ksh_lineage to .sh/.ksh
        if (src.match(/_sh_lineage$/i)) {
            src = src.replace(/_sh_lineage$/i, '.sh');
        } else if (src.match(/_ksh_lineage$/i)) {
            src = src.replace(/_ksh_lineage$/i, '.ksh');
        } else if (src.match(/_sql_lineage$/i)) {
            src = src.replace(/_sql_lineage$/i, '.sql');
        } else if (src.match(/_lineage$/i)) {
            src = src.replace(/_lineage$/i, '');
        }
        scriptDisplay = src;
    } else if (lineageData.source_file) {
        // Fallback for legacy single file mode
        let src = lineageData.source_file;
        src = src.split('/').pop();
        src = src.replace(/\.json$/i, '');
        if (src.match(/_sh_lineage$/i)) {
            src = src.replace(/_sh_lineage$/i, '.sh');
        } else if (src.match(/_ksh_lineage$/i)) {
            src = src.replace(/_ksh_lineage$/i, '.ksh');
        } else if (src.match(/_sql_lineage$/i)) {
            src = src.replace(/_sql_lineage$/i, '.sql');
        } else if (src.match(/_lineage$/i)) {
            src = src.replace(/_lineage$/i, '');
        }
        scriptDisplay = src;
    }

    // Add scripts card
    let scriptsInfo = '';
    if (scriptCount === 1 && scriptDisplay) {
        scriptsInfo = `<div class="summary-card">
            <h3>Scripts</h3>
            <div class="number">1</div>
            <div style="color:#1976d2; font-size:1.1em; font-weight:bold; margin-top:8px; word-break:break-all;">${scriptDisplay}</div>
        </div>`;
    } else {
        scriptsInfo = `<div class="summary-card">
            <h3>Scripts</h3>
            <div class="number">${scriptCount}</div>
        </div>`;
    }

    summaryGrid.innerHTML = `
        <div class="summary-card">
            <h3>Total Tables</h3>
            <div class="number">${totalTables}</div>
        </div>
        <div class="summary-card">
            <h3>Source Tables</h3>
            <div class="number">${sourceTables}</div>
        </div>
        <div class="summary-card">
            <h3>Target Tables</h3>
            <div class="number">${targetTables}</div>
        </div>
        <div class="summary-card">
            <h3>Volatile Tables</h3>
            <div class="number">${volatileTables}</div>
        </div>
        <div class="summary-card">
            <h3>SQL Operations</h3>
            <div class="number">${totalOperations}</div>
        </div>
        ${scriptsInfo}
    `;
}

function displayTables() {
    const tableList = document.getElementById('tableList');
    
    // Check if we have file groups (folder mode) or single file
    if (lineageData.file_groups) {
        // Tree view for multiple files
        tableList.innerHTML = `
            <div class="tree-view">
                ${Object.keys(lineageData.file_groups).map(fileName => {
                    const fileGroup = lineageData.file_groups[fileName];
                    const tables = fileGroup.tables.sort();
                    const scriptName = fileName;
                    

                    return `
                        <div class="file-group">
                            <div class="tree-toggle" onclick="toggleFileGroup('${fileName}-tables')">
                                <span class="toggle-icon">▶</span>
                                <span>📄 ${scriptName} (${tables.length})</span>
                            </div>
                            <div class="tree-children" id="${fileName}-tables">
                                ${tables.map(tableName => {
                                    const table = lineageData.tables[tableName];
                                    const isVolatile = table.is_volatile;
                                    const className = `tree-item ${isVolatile ? 'volatile' : ''}`;
                                    
                                    return `
                                        <div class="${className}" onclick="selectTable('${tableName}')">
                                            <div style="font-weight: bold;">${tableName}</div>
                                            <div style="font-size: 0.8em; color: #6c757d;">
                                                ${table.source.length} sources, ${table.target.length} targets
                                                ${isVolatile ? ' (volatile)' : ''}
                                            </div>
                                        </div>
                                    `;
                                }).join('')}
                            </div>
                        </div>
                    `;
                }).join('')}
            </div>
        `;
    } else {
        // Simple list for single file
        const tables = Object.keys(lineageData.tables).sort();
        
        tableList.innerHTML = tables.map(tableName => {
            const table = lineageData.tables[tableName];
            const isVolatile = table.is_volatile;
            const className = `table-item ${isVolatile ? 'volatile' : ''}`;
            
            return `
                <li class="${className}" onclick="selectTable('${tableName}')">
                    <div style="font-weight: bold;">${tableName}</div>
                    <div style="font-size: 0.8em; color: #6c757d;">
                        ${table.source.length} sources, ${table.target.length} targets
                        ${isVolatile ? ' (volatile)' : ''}
                    </div>
                </li>
            `;
        }).join('');
    }
}

function displayStatements() {
    const statementList = document.getElementById('statementList');
    
    // Check if we have file groups (folder mode) or single file
    if (lineageData.file_groups) {
        // Tree view for multiple files
        statementList.innerHTML = `
            <div class="tree-view">
                ${Object.keys(lineageData.file_groups).map(fileName => {
                    const fileGroup = lineageData.file_groups[fileName];
                    const statements = fileGroup.statements;
                    if (statements.length === 0) return '';

                    // Use the file group key (fileName) as the script name and clean it up
                    let scriptName = fileName;
                    scriptName = scriptName.split('/').pop();
                    scriptName = scriptName.replace(/\.json$/i, '');
                    if (scriptName.match(/_sh_lineage$/i)) {
                        scriptName = scriptName.replace(/_sh_lineage$/i, '.sh');
                    } else if (scriptName.match(/_ksh_lineage$/i)) {
                        scriptName = scriptName.replace(/_ksh_lineage$/i, '.ksh');
                    } else if (scriptName.match(/_sql_lineage$/i)) {
                        scriptName = scriptName.replace(/_sql_lineage$/i, '.sql');
                    } else if (scriptName.match(/_lineage$/i)) {
                        scriptName = scriptName.replace(/_lineage$/i, '');
                    }

                    return `
                        <div class="file-group">
                            <div class="tree-toggle" onclick="toggleFileGroup('${fileName}-statements')">
                                <span class="toggle-icon">▶</span>
                                <span>📄 ${scriptName} (${statements.length})</span>
                            </div>
                            <div class="tree-children" id="${fileName}-statements">
                                ${statements.map((globalIndex, localIndex) => {
                                    const statement = lineageData.bteq_statements[globalIndex];
                                    const firstLine = statement.split('\n')[0].trim();
                                    const displayText = firstLine.length > 60 ? firstLine.substring(0, 60) + '...' : firstLine;
                                    return `
                                        <div class="tree-item" onclick="selectStatementByScript('${fileName}', ${localIndex})">
                                            <div style="font-weight: bold;">Statement ${scriptName}:${localIndex}</div>
                                            <div style="font-size: 0.8em; color: #6c757d;">
                                                ${displayText}
                                            </div>
                                        </div>
                                    `;
                                }).join('')}
                            </div>
                        </div>
                    `;
                }).join('')}
            </div>
        `;
    } else {
        // Simple list for single file
        const statements = lineageData.bteq_statements;
        let scriptName = Object.keys(lineageData.file_groups)[0];
        scriptName = scriptName.split('/').pop();
        scriptName = scriptName.replace(/\.json$/i, '');
        if (scriptName.match(/_sh_lineage$/i)) {
            scriptName = scriptName.replace(/_sh_lineage$/i, '.sh');
        } else if (scriptName.match(/_ksh_lineage$/i)) {
            scriptName = scriptName.replace(/_ksh_lineage$/i, '.ksh');
        } else if (scriptName.match(/_sql_lineage$/i)) {
            scriptName = scriptName.replace(/_sql_lineage$/i, '.sql');
        } else if (scriptName.match(/_lineage$/i)) {
            scriptName = scriptName.replace(/_lineage$/i, '');
        }
        statementList.innerHTML = statements.map((statement, index) => {
            const firstLine = statement.split('\n')[0].trim();
            const displayText = firstLine.length > 60 ? firstLine.substring(0, 60) + '...' : firstLine;
            return `
                <li class="statement-item" onclick="selectStatementByScript('${Object.keys(lineageData.file_groups)[0]}', ${index})">
                    <div style="font-weight: bold;">${scriptName}:${index}</div>
                    <div style="font-size: 0.8em; color: #6c757d;">
                        ${displayText}
                    </div>
                </li>
            `;
        }).join('');
    }
}

// New function to select statement by script and local index
function selectStatementByScript(scriptKey, localIndex) {
    // Update selected state
    document.querySelectorAll('.statement-item, .tree-item').forEach(item => {
        item.classList.remove('selected');
    });
    event.target.closest('.statement-item, .tree-item').classList.add('selected');
    // Clear table selection
    document.querySelectorAll('.table-item, .tree-item').forEach(item => {
        item.classList.remove('selected');
    });
    displayStatementDetailsByScript(scriptKey, localIndex);
}

// New function to display statement details by script and local index
function displayStatementDetailsByScript(scriptKey, localIndex) {
    const contentArea = document.getElementById('statementContentArea');
    const fileGroup = lineageData.file_groups[scriptKey];
    if (!fileGroup) {
        contentArea.innerHTML = `<div class="error">Script not found.</div>`;
        return;
    }
    const globalIndex = fileGroup.statements[localIndex];
    const statement = lineageData.bteq_statements[globalIndex];
    // Clean up script name for display
    let scriptName = scriptKey.split('/').pop();
    scriptName = scriptName.replace(/\.json$/i, '');
    if (scriptName.match(/_sh_lineage$/i)) {
        scriptName = scriptName.replace(/_sh_lineage$/i, '.sh');
    } else if (scriptName.match(/_ksh_lineage$/i)) {
        scriptName = scriptName.replace(/_ksh_lineage$/i, '.ksh');
    } else if (scriptName.match(/_sql_lineage$/i)) {
        scriptName = scriptName.replace(/_sql_lineage$/i, '.sql');
    } else if (scriptName.match(/_lineage$/i)) {
        scriptName = scriptName.replace(/_lineage$/i, '');
    }
    contentArea.innerHTML = `
        <div class="table-details">
            <div class="table-header">
                <div class="table-name">SQL Statement ${scriptName}:${localIndex}</div>
            </div>
            <div style="background: #f8f9fa; padding: 20px; border-radius: 8px; margin-top: 20px;">
                <h4 style="margin-bottom: 15px; color: #495057;">Formatted SQL:</h4>
                <div style="background: white; padding: 20px; border-radius: 8px; font-family: 'Courier New', monospace; white-space: pre-wrap; font-size: 14px; line-height: 1.5; max-height: 500px; overflow-y: auto; border: 1px solid #dee2e6;">
${statement}
                </div>
            </div>
        </div>
    `;
}

function displayNetworkFileGroups() {
    const networkFileGroups = document.getElementById('networkFileGroups');
    
    if (!lineageData.file_groups) {
        networkFileGroups.innerHTML = '<p style="color: #6c757d; font-size: 0.9em;">Single file mode - use "All Tables Network"</p>';
        return;
    }
    
    networkFileGroups.innerHTML = `
        <div class="tree-view">
            ${Object.keys(lineageData.file_groups).map(fileName => {
                const fileGroup = lineageData.file_groups[fileName];
                const tables = fileGroup.tables;
                if (tables.length === 0) return '';

                // Use the file group key (fileName) as the script name and clean it up
                let scriptName = fileName;
                // Remove path if present
                scriptName = scriptName.split('/').pop();
                // Remove .json extension if present
                scriptName = scriptName.replace(/\.json$/i, '');
                // Convert *_sh_lineage or *_ksh_lineage to .sh/.ksh
                if (scriptName.match(/_sh_lineage$/i)) {
                    scriptName = scriptName.replace(/_sh_lineage$/i, '.sh');
                } else if (scriptName.match(/_ksh_lineage$/i)) {
                    scriptName = scriptName.replace(/_ksh_lineage$/i, '.ksh');
                } else if (scriptName.match(/_sql_lineage$/i)) {
                    scriptName = scriptName.replace(/_sql_lineage$/i, '.sql');
                } else if (scriptName.match(/_lineage$/i)) {
                    scriptName = scriptName.replace(/_lineage$/i, '');
                }

                return `
                    <div class="file-group">
                        <div class="tree-toggle" onclick="toggleFileGroup('${fileName}-network')">
                            <span class="toggle-icon">▶</span>
                            <span style="cursor:pointer;" onclick="event.stopPropagation(); showFileNetwork('${fileName}')">📄 ${scriptName} (${tables.length})</span>
                        </div>
                        <div class="tree-children" id="${fileName}-network">
                            ${tables.map(tableName => {
                                const table = lineageData.tables[tableName];
                                const isVolatile = table.is_volatile;
                                const className = `tree-item ${isVolatile ? 'volatile' : ''}`;
                                
                                return `
                                    <div class="${className}" onclick="showTableNetwork('${tableName}')">
                                        <div style="font-weight: bold;">${tableName}</div>
                                        <div style="font-size: 0.8em; color: #6c757d;">
                                            ${table.source.length} sources, ${table.target.length} targets
                                            ${isVolatile ? ' (volatile)' : ''}
                                        </div>
                                    </div>
                                `;
                            }).join('')}
                        </div>
                    </div>
                `;
            }).join('')}
        </div>
    `;
}

function selectTable(tableName) {
    // Update selected state
    document.querySelectorAll('.table-item, .tree-item').forEach(item => {
        item.classList.remove('selected');
    });
    event.target.closest('.table-item, .tree-item').classList.add('selected');
    
    // Clear statement selection
    document.querySelectorAll('.statement-item, .tree-item').forEach(item => {
        item.classList.remove('selected');
    });
    
    selectedTable = tableName;
    displayTableDetails(tableName);
}

function selectStatement(statementIndex) {
    // Update selected state
    document.querySelectorAll('.statement-item, .tree-item').forEach(item => {
        item.classList.remove('selected');
    });
    event.target.closest('.statement-item, .tree-item').classList.add('selected');
    
    // Clear table selection
    document.querySelectorAll('.table-item, .tree-item').forEach(item => {
        item.classList.remove('selected');
    });
    
    displayStatementDetails(statementIndex);
}

function deselectAll() {
    // Clear all selections
    document.querySelectorAll('.table-item, .tree-item, .statement-item').forEach(item => {
        item.classList.remove('selected');
    });
    
    selectedTable = null;
    
    // Show default content
    const contentArea = document.getElementById('contentArea');
    const statementContentArea = document.getElementById('statementContentArea');
    
    if (contentArea) {
        contentArea.innerHTML = `
            <div class="loading">
                <h3>📁 Select a table to view its lineage details</h3>
                <p>Click on any table in the sidebar to see its source and target relationships</p>
                <p style="margin-top: 20px; font-size: 0.9em; color: #6c757d;">
                    <strong>Tip:</strong> Press <kbd>Escape</kbd> or click on empty space to deselect
                </p>
            </div>
        `;
    }
    
    if (statementContentArea) {
        statementContentArea.innerHTML = `
            <div class="loading">
                <h3>📁 Select a statement to view its SQL details</h3>
                <p>Click on any statement in the sidebar to see its SQL content</p>
                <p style="margin-top: 20px; font-size: 0.9em; color: #6c757d;">
                    <strong>Tip:</strong> Press <kbd>Escape</kbd> or click on empty space to deselect
                </p>
            </div>
        `;
    }
}

function toggleFileGroup(groupId) {
    const toggle = event.target.closest('.tree-toggle');
    const children = document.getElementById(groupId);
    
    if (toggle.classList.contains('expanded')) {
        toggle.classList.remove('expanded');
        children.classList.remove('expanded');
    } else {
        toggle.classList.add('expanded');
        children.classList.add('expanded');
    }
}

function displayStatementDetails(statementIndex) {
    const contentArea = document.getElementById('statementContentArea');
    const statement = lineageData.bteq_statements[statementIndex];
    const statementInfo = getStatementInfo(statementIndex);
    
    contentArea.innerHTML = `
        <div class="table-details">
            <div class="table-header">
                <div class="table-name">SQL Statement ${statementInfo.displayText}</div>
            </div>
            
            <div style="background: #f8f9fa; padding: 20px; border-radius: 8px; margin-top: 20px;">
                <h4 style="margin-bottom: 15px; color: #495057;">Formatted SQL:</h4>
                <div style="background: white; padding: 20px; border-radius: 8px; font-family: 'Courier New', monospace; white-space: pre-wrap; font-size: 14px; line-height: 1.5; max-height: 500px; overflow-y: auto; border: 1px solid #dee2e6;">
${statement}
                </div>
            </div>
        </div>
    `;
}

function displayTableDetails(tableName) {
    const contentArea = document.getElementById('contentArea');
    const table = lineageData.tables[tableName];
    
    const sourceRelationships = table.source.map(rel => `
        <div class="relationship-item">
            <div class="table-name">${rel.name}</div>
            <div class="operations">
                Operations: 
                <div class="operation-list">
                    ${rel.operation.map(opIndex => `
                        <span class="operation-badge" onclick="showSql(${opIndex})">
                            ${opIndex}
                        </span>
                    `).join('')}
                </div>
            </div>
        </div>
    `).join('');

    const targetRelationships = table.target.map(rel => `
        <div class="relationship-item">
            <div class="table-name">${rel.name}</div>
            <div class="operations">
                Operations: 
                <div class="operation-list">
                    ${rel.operation.map(opIndex => `
                        <span class="operation-badge" onclick="showSql(${opIndex})">
                            ${opIndex}
                        </span>
                    `).join('')}
                </div>
            </div>
        </div>
    `).join('');

    contentArea.innerHTML = `
        <div class="table-details">
            <div class="table-header">
                <div class="table-name">${tableName}</div>
                ${table.is_volatile ? '<div class="volatile-badge">VOLATILE</div>' : ''}
            </div>
            
            <div class="relationships">
                <div class="relationship-section">
                    <h4>📥 Source Tables (${table.source.length})</h4>
                    ${table.source.length > 0 ? sourceRelationships : '<p style="color: #6c757d;">No source tables</p>'}
                </div>
                
                <div class="relationship-section">
                    <h4>📤 Target Tables (${table.target.length})</h4>
                    ${table.target.length > 0 ? targetRelationships : '<p style="color: #6c757d;">No target tables</p>'}
                </div>
            </div>
        </div>
    `;
}

function showSql(operationIndex) {
    const modal = document.getElementById('sqlModal');
    const modalTitle = document.getElementById('modalTitle');
    const sqlContent = document.getElementById('sqlContent');
    const statementInfo = getStatementInfo(operationIndex);
    
    modalTitle.textContent = `SQL Statement ${statementInfo.displayText}`;
    sqlContent.textContent = lineageData.bteq_statements[operationIndex];
    modal.style.display = 'block';
}

function closeSqlModal() {
    document.getElementById('sqlModal').style.display = 'none';
}

function showError(message) {
    const contentArea = document.getElementById('contentArea');
    contentArea.innerHTML = `
        <div class="error">
            <strong>Error:</strong> ${message}
        </div>
    `;
}

function switchTab(tabName) {
    // Update tab buttons
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.classList.remove('active');
    });
    event.target.classList.add('active');
    
    // Update tab content
    document.querySelectorAll('.tab-content').forEach(content => {
        content.classList.remove('active');
    });
    
    if (tabName === 'tables') {
        document.getElementById('tablesTab').classList.add('active');
    } else if (tabName === 'statements') {
        document.getElementById('statementsTab').classList.add('active');
    } else if (tabName === 'network') {
        document.getElementById('networkTab').classList.add('active');
        if (lineageData && !network) {
            setTimeout(() => {
                createNetworkVisualization();
            }, 100);
        }
    }
}

// Helper function to determine node color based on edge relationships and volatility
function getNodeColor(table) {
    if (table.is_volatile) {
        return '#ff9800'; // Yellow for volatile tables
    } else if (table.source.length === 0) {
        return '#28a745'; // Green for tables with no in-edges (no sources)
    } else if (table.target.length === 0) {
        return '#dc3545'; // Red for tables with no out-edges (no targets)
    } else {
        return '#007bff'; // Blue for all other tables
    }
}

function createNetworkVisualization() {
    const container = document.getElementById('networkContainer');
    
    // Create nodes (tables)
    const nodes = [];
    const nodeIds = new Set();
    
    // Add all tables as nodes
    Object.keys(lineageData.tables).forEach(tableName => {
        const table = lineageData.tables[tableName];
        nodeIds.add(tableName);
        
        const node = {
            id: tableName,
            label: tableName,
            title: `${tableName}\nSources: ${table.source.length}\nTargets: ${table.target.length}\nVolatile: ${table.is_volatile ? 'Yes' : 'No'}`,
            color: getNodeColor(table),
            size: 20 + Math.min(table.source.length + table.target.length, 10) * 2,
            font: {
                size: 12,
                face: 'Arial'
            }
        };
        nodes.push(node);
    });
    
    // Create edges (relationships)
    const edges = [];
    const edgeMap = new Map(); // To avoid duplicate edges
    
    Object.keys(lineageData.tables).forEach(tableName => {
        const table = lineageData.tables[tableName];
        
        // Add source relationships
        table.source.forEach(sourceRel => {
            const edgeKey = `${sourceRel.name}->${tableName}`;
            if (!edgeMap.has(edgeKey)) {
                edgeMap.set(edgeKey, true);
                
                const operations = getOperationDisplayText([{ scriptName: table.script_name || 'Unknown', indices: sourceRel.operation }]);
                edges.push({
                    from: sourceRel.name,
                    to: tableName,
                    arrows: 'to',
                    color: { color: '#28a745', opacity: 0.8 },
                    width: 2,
                    title: `Operations: ${operations}`,
                    label: operations,
                    font: {
                        size: 10,
                        color: '#28a745'
                    }
                });
            }
        });
        
        // Add target relationships
        table.target.forEach(targetRel => {
            const edgeKey = `${tableName}->${targetRel.name}`;
            if (!edgeMap.has(edgeKey)) {
                edgeMap.set(edgeKey, true);
                
                const operations = getOperationDisplayText([{ scriptName: table.script_name || 'Unknown', indices: targetRel.operation }]);
                edges.push({
                    from: tableName,
                    to: targetRel.name,
                    arrows: 'to',
                    color: { color: '#dc3545', opacity: 0.8 },
                    width: 2,
                    title: `Operations: ${operations}`,
                    label: operations,
                    font: {
                        size: 10,
                        color: '#dc3545'
                    }
                });
            }
        });
    });
    
    // Create the network
    const data = {
        nodes: new vis.DataSet(nodes),
        edges: new vis.DataSet(edges)
    };
    
    const options = {
        nodes: {
            shape: 'box',
            borderWidth: 2,
            shadow: true
        },
        edges: {
            smooth: {
                type: 'curvedCW',
                roundness: 0.2
            }
        },
        physics: {
            enabled: false
        },
        interaction: {
            hover: true,
            tooltipDelay: 200,
            zoomView: true,
            dragView: true
        },
        layout: {
            improvedLayout: true
        }
    };
    
    network = new vis.Network(container, data, options);
    
    // Add click events for nodes and edges
    network.on('click', function(params) {
        if (params.nodes.length > 0) {
            // Node clicked - apply filter to show this table and its relationships
            const clickedNodeId = params.nodes[0];
            showDirectlyRelatedNodes(clickedNodeId);
            hideSidePanel();
        } else if (params.edges.length > 0) {
            // Edge clicked
            showEdgeDetails(params.edges[0]);
        } else {
            hideSidePanel();
        }
    });
}

function loadAllLineageFiles() {
    // Load the all_lineage.txt file and process all JSON files listed in it
    fetch('../report/all_lineage.txt')
        .then(response => {
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            return response.text();
        })
        .then(text => {
            const jsonFiles = text.trim().split('\n').filter(line => line.length > 0);
            console.log(`Found ${jsonFiles.length} JSON files in all_lineage.txt`);
            
            if (jsonFiles.length === 0) {
                showError('No JSON files found in all_lineage.txt');
                return;
            }
            
            // Initialize merged data structure
            const mergedData = {
                tables: {},
                bteq_statements: [],
                source_file: `${jsonFiles.length}`,
                file_groups: {}
            };
            
            let processedFiles = 0;
            const totalFiles = jsonFiles.length;
            
            // Process each JSON file
            jsonFiles.forEach((jsonFile, index) => {
                // Use the JSON file path as-is (relative to all_lineage.txt location)
                fetch(`../report/${jsonFile}`)
                    .then(response => {
                        if (!response.ok) {
                            throw new Error(`HTTP error! status: ${response.status}`);
                        }
                        return response.json();
                    })
                    .then(data => {
                        console.log(`Processing file: ${jsonFile}`);
                        
                        // Create file group using script_name from the data
                        const scriptName = data.script_name || jsonFile.replace('_lineage.json', '');
                        mergedData.file_groups[scriptName] = {
                            tables: [],
                            statements: []
                        };
                        
                        // Merge tables with file tracking
                        if (data.tables) {
                            Object.keys(data.tables).forEach(tableName => {
                                mergedData.tables[tableName] = data.tables[tableName];
                                // Add script_name to the table object
                                mergedData.tables[tableName].script_name = scriptName;
                                mergedData.file_groups[scriptName].tables.push(tableName);
                            });
                        }
                        
                        // Merge BTEQ statements with file tracking
                        if (data.bteq_statements) {
                            const startIndex = mergedData.bteq_statements.length;
                            mergedData.bteq_statements.push(...data.bteq_statements);
                            
                            // Track statement indices for this file
                            for (let i = 0; i < data.bteq_statements.length; i++) {
                                mergedData.file_groups[scriptName].statements.push(startIndex + i);
                            }
                        }
                        
                        processedFiles++;
                        
                        // When all files are processed, update the display
                        if (processedFiles === totalFiles) {
                                                        console.log(`Successfully merged ${totalFiles} files`);
                    lineageData = mergedData;
                    buildScriptAwareMappings();
                    displaySummary();
                    displayTables();
                    displayStatements();
                    displayNetworkFileGroups();
                    document.getElementById('summarySection').style.display = 'block';
                    document.getElementById('tabSection').style.display = 'block';
                    initializeTableNames();
                        }
                    })
                    .catch(error => {
                        console.error(`Error processing JSON file ${jsonFile}:`, error);
                        processedFiles++;
                        
                        if (processedFiles === totalFiles) {
                            showError(`Error processing some files. Check console for details.`);
                        }
                    });
            });
        })
        .catch(error => {
            showError('Error loading all_lineage.txt. Make sure the file exists in the ../report/ directory: ' + error.message);
        });
}

function highlightConnectedNodes(nodeId) {
    const connectedNodes = new Set();
    const connectedEdges = new Set();
    
    // Find all connected nodes and edges
    lineageData.tables[nodeId].source.forEach(sourceRel => {
        connectedNodes.add(sourceRel.name);
    });
    
    lineageData.tables[nodeId].target.forEach(targetRel => {
        connectedNodes.add(targetRel.name);
    });
    
    // Highlight the selected node and its connections
    network.selectNodes([nodeId]);
    
    // You can add more highlighting logic here if needed
}

function showMultipleTables(tableNames) {
    console.log('showMultipleTables called with:', tableNames);
    const container = document.getElementById('networkContainer');
    
    // Get all related tables for the selected tables
    const relatedTables = new Set();
    
    tableNames.forEach(tableName => {
        if (lineageData.tables[tableName]) {
            relatedTables.add(tableName);
            const table = lineageData.tables[tableName];
            
            // Add source tables (tables that provide data to this table)
            table.source.forEach(sourceRel => {
                relatedTables.add(sourceRel.name);
            });
            
            // Add target tables (tables that receive data from this table)
            table.target.forEach(targetRel => {
                relatedTables.add(targetRel.name);
            });
        }
    });
    
    // Create nodes for related tables only
    const nodes = [];
    const edges = [];
    const edgeMap = new Map();
    
    relatedTables.forEach(tableName => {
        if (lineageData.tables[tableName]) {
            const tableData = lineageData.tables[tableName];
            const isSelected = tableNames.includes(tableName);
            
            const node = {
                id: tableName,
                label: tableName,
                title: `${tableName}\nSources: ${tableData.source.length}\nTargets: ${tableData.target.length}\nVolatile: ${tableData.is_volatile ? 'Yes' : 'No'}`,
                color: isSelected ? '#007bff' : getNodeColor(tableData),
                size: isSelected ? 30 : (20 + Math.min(tableData.source.length + tableData.target.length, 10) * 2),
                font: {
                    size: isSelected ? 16 : 12,
                    face: 'Arial',
                    bold: isSelected
                },
                borderWidth: isSelected ? 3 : 2
            };
            nodes.push(node);
            
            // Add edges only between related tables
            tableData.source.forEach(sourceRel => {
                if (relatedTables.has(sourceRel.name)) {
                    const edgeKey = `${sourceRel.name}->${tableName}`;
                    if (!edgeMap.has(edgeKey)) {
                        edgeMap.set(edgeKey, true);
                        
                        const operations = getOperationDisplayText([{ scriptName: tableData.script_name || 'Unknown', indices: sourceRel.operation }]);
                        edges.push({
                            from: sourceRel.name,
                            to: tableName,
                            arrows: 'to',
                            color: { color: '#28a745', opacity: 0.8 },
                            width: 2,
                            title: `Operations: ${operations}`,
                            label: operations,
                            font: {
                                size: 10,
                                color: '#28a745'
                            }
                        });
                    }
                }
            });
            
            tableData.target.forEach(targetRel => {
                if (relatedTables.has(targetRel.name)) {
                    const edgeKey = `${tableName}->${targetRel.name}`;
                    if (!edgeMap.has(edgeKey)) {
                        edgeMap.set(edgeKey, true);
                        
                        const operations = getOperationDisplayText([{ scriptName: tableData.script_name || 'Unknown', indices: targetRel.operation }]);
                        edges.push({
                            from: tableName,
                            to: targetRel.name,
                            arrows: 'to',
                            color: { color: '#dc3545', opacity: 0.8 },
                            width: 2,
                            title: `Operations: ${operations}`,
                            label: operations,
                            font: {
                                size: 10,
                                color: '#dc3545'
                            }
                        });
                    }
                }
            });
        }
    });
    
    // Update the network with filtered data
    const data = {
        nodes: new vis.DataSet(nodes),
        edges: new vis.DataSet(edges)
    };
    
    const options = {
        nodes: {
            shape: 'box',
            borderWidth: 2,
            shadow: true
        },
        edges: {
            smooth: {
                type: 'curvedCW',
                roundness: 0.2
            }
        },
        physics: {
            enabled: false
        },
        interaction: {
            hover: true,
            tooltipDelay: 200,
            zoomView: true,
            dragView: true
        },
        layout: {
            improvedLayout: true
        }
    };
    
    if (network) {
        network.destroy();
    }
    
    network = new vis.Network(container, data, options);
    
    // Add click events for the filtered network
    network.on('click', function(params) {
        if (params.nodes.length > 0) {
            // Node clicked - apply filter to show this table and its relationships
            const clickedNodeId = params.nodes[0];
            showDirectlyRelatedNodes(clickedNodeId);
            hideSidePanel();
        } else if (params.edges.length > 0) {
            // Edge clicked
            showEdgeDetails(params.edges[0]);
        } else {
            hideSidePanel();
        }
    });
}

function showDirectlyRelatedNodes(nodeId) {
    const container = document.getElementById('networkContainer');
    const table = lineageData.tables[nodeId];
    
    // Get directly related tables
    const relatedTables = new Set([nodeId]);
    
    // Add source tables (tables that provide data to this table)
    table.source.forEach(sourceRel => {
        relatedTables.add(sourceRel.name);
    });
    
    // Add target tables (tables that receive data from this table)
    table.target.forEach(targetRel => {
        relatedTables.add(targetRel.name);
    });
    
    // Create nodes for related tables only
    const nodes = [];
    const edges = [];
    const edgeMap = new Map();
    
    relatedTables.forEach(tableName => {
        if (lineageData.tables[tableName]) {
            const tableData = lineageData.tables[tableName];
            const isSelected = tableName === nodeId;
            
            const node = {
                id: tableName,
                label: tableName,
                title: `${tableName}\nSources: ${tableData.source.length}\nTargets: ${tableData.target.length}\nVolatile: ${tableData.is_volatile ? 'Yes' : 'No'}`,
                color: isSelected ? '#007bff' : getNodeColor(tableData),
                size: isSelected ? 30 : (20 + Math.min(tableData.source.length + tableData.target.length, 10) * 2),
                font: {
                    size: isSelected ? 16 : 12,
                    face: 'Arial',
                    bold: isSelected
                },
                borderWidth: isSelected ? 3 : 2
            };
            nodes.push(node);
            
            // Add edges only between related tables
            tableData.source.forEach(sourceRel => {
                if (relatedTables.has(sourceRel.name)) {
                    const edgeKey = `${sourceRel.name}->${tableName}`;
                    if (!edgeMap.has(edgeKey)) {
                        edgeMap.set(edgeKey, true);
                        
                        const operations = getOperationDisplayText([{ scriptName: tableData.script_name || 'Unknown', indices: sourceRel.operation }]);
                        edges.push({
                            from: sourceRel.name,
                            to: tableName,
                            arrows: 'to',
                            color: { color: '#28a745', opacity: 0.8 },
                            width: 2,
                            title: `Operations: ${operations}`,
                            label: operations,
                            font: {
                                size: 10,
                                color: '#28a745'
                            }
                        });
                    }
                }
            });
            
            tableData.target.forEach(targetRel => {
                if (relatedTables.has(targetRel.name)) {
                    const edgeKey = `${tableName}->${targetRel.name}`;
                    if (!edgeMap.has(edgeKey)) {
                        edgeMap.set(edgeKey, true);
                        
                        const operations = getOperationDisplayText([{ scriptName: tableData.script_name || 'Unknown', indices: targetRel.operation }]);
                        edges.push({
                            from: tableName,
                            to: targetRel.name,
                            arrows: 'to',
                            color: { color: '#dc3545', opacity: 0.8 },
                            width: 2,
                            title: `Operations: ${operations}`,
                            label: operations,
                            font: {
                                size: 10,
                                color: '#dc3545'
                            }
                        });
                    }
                }
            });
        }
    });
    
    // Update the network with filtered data
    const data = {
        nodes: new vis.DataSet(nodes),
        edges: new vis.DataSet(edges)
    };
    
    const options = {
        nodes: {
            shape: 'box',
            borderWidth: 2,
            shadow: true
        },
        edges: {
            smooth: {
                type: 'curvedCW',
                roundness: 0.2
            }
        },
        physics: {
            enabled: false
        },
        interaction: {
            hover: true,
            tooltipDelay: 200,
            zoomView: true,
            dragView: true
        },
        layout: {
            improvedLayout: true
        }
    };
    
    if (network) {
        network.destroy();
    }
    
    network = new vis.Network(container, data, options);
    
    // Add click events for the filtered network
    network.on('click', function(params) {
        if (params.nodes.length > 0) {
            // Node clicked - apply filter to show this table and its relationships
            const clickedNodeId = params.nodes[0];
            showDirectlyRelatedNodes(clickedNodeId);
            hideSidePanel();
        } else if (params.edges.length > 0) {
            // Edge clicked
            showEdgeDetails(params.edges[0]);
        } else {
            hideSidePanel();
        }
    });
}

function showAllNodes() {
    // Recreate the full network visualization
    createNetworkVisualization();
}

function showEdgeDetails(edgeId) {
    const sidePanel = document.getElementById('networkSidePanel');
    const sidePanelTitle = document.getElementById('sidePanelTitle');
    const sidePanelContent = document.getElementById('sidePanelContent');
    
    // Get the edge data
    const edge = network.body.data.edges.get(edgeId);
    const fromTable = edge.from;
    const toTable = edge.to;
    const label = edge.label;
    // New format: 'ScriptA.sh:0|1|2|3|4|5|6|7|8|9, ScriptB.sh:1'
    // Parse into [{scriptName, indices: [..]}, ...]
    let opGroups = [];
    label.split(',').forEach(part => {
        const trimmed = part.trim();
        const match = trimmed.match(/^(.*?):([\d|]+)$/);
        if (match) {
            const scriptName = match[1];
            const indices = match[2].split('|').map(x => parseInt(x, 10)).filter(x => !isNaN(x));
            if (indices.length > 0) {
                opGroups.push({scriptName, indices});
            }
        }
    });
    // Update title
    sidePanelTitle.textContent = `${fromTable} → ${toTable}`;
    // Create content with SQL statements
    let content = `
        <div style="margin-bottom: 20px;">
            <h4 style="color: #495057; margin-bottom: 10px;">Data Flow</h4>
            <p><strong>From:</strong> ${fromTable}</p>
            <p><strong>To:</strong> ${toTable}</p>
            <p><strong>Operations:</strong> ${label}</p>
        </div>
        <div style="border-top: 1px solid #dee2e6; padding-top: 20px;">
            <h4 style="color: #495057; margin-bottom: 15px;">SQL Statements</h4>
    `;
    // Add each SQL statement
    opGroups.forEach(group => {
        let fileGroup = null;
        if (lineageData.file_groups && lineageData.file_groups[group.scriptName]) {
            fileGroup = lineageData.file_groups[group.scriptName];
        } else if (lineageData.file_groups) {
            // Try to find by normalized script name
            for (const [key, fg] of Object.entries(lineageData.file_groups)) {
                let normKey = key.split('/').pop().replace(/\.json$/i, '');
                if (normKey.match(/_sh_lineage$/i)) normKey = normKey.replace(/_sh_lineage$/i, '.sh');
                else if (normKey.match(/_ksh_lineage$/i)) normKey = normKey.replace(/_ksh_lineage$/i, '.ksh');
                else if (normKey.match(/_sql_lineage$/i)) normKey = normKey.replace(/_sql_lineage$/i, '.sql');
                else if (normKey.match(/_lineage$/i)) normKey = normKey.replace(/_lineage$/i, '');
                if (normKey === group.scriptName) {
                    fileGroup = fg;
                    break;
                }
            }
        }
        group.indices.forEach(localIdx => {
            let globalIndex = null;
            if (fileGroup && fileGroup.statements && fileGroup.statements.length > localIdx) {
                globalIndex = fileGroup.statements[localIdx];
            } else if (!lineageData.file_groups && lineageData.bteq_statements.length > localIdx) {
                globalIndex = localIdx;
            }
            if (globalIndex !== null && !isNaN(globalIndex)) {
                const sqlStatement = lineageData.bteq_statements[globalIndex];
                content += `
                    <div style=\"background: white; padding: 15px; border-radius: 8px; margin-bottom: 15px; border: 1px solid #dee2e6;\">
                        <h5 style=\"color: #007bff; margin-bottom: 10px;\">${group.scriptName}:${localIdx}</h5>
                        <div style=\"background: #f8f9fa; padding: 12px; border-radius: 6px; font-family: 'Courier New', monospace; white-space: pre-wrap; font-size: 12px; line-height: 1.4; max-height: 200px; overflow-y: auto; border: 1px solid #e9ecef;\">
${sqlStatement}
                        </div>
                    </div>
                `;
            }
        });
    });
    content += `</div>`;
    sidePanelContent.innerHTML = content;
    sidePanel.style.display = 'block';
}

function showAllTablesNetwork() {
    selectedNetworkScript = null;
    updateSelectedScriptLabel();
    createNetworkVisualization();
}

function showTableNetwork(tableName) {
    // Show the selected table along with its directly related tables (sources and targets)
    showDirectlyRelatedNodes(tableName);
}

function showFileNetwork(fileName) {
    selectedNetworkScript = fileName;
    updateSelectedScriptLabel();
    if (!lineageData.file_groups || !lineageData.file_groups[fileName]) {
        return;
    }
    const tables = lineageData.file_groups[fileName].tables;
    const tablePairs = tables.map(tableName => {
        const table = lineageData.tables[tableName];
        const scriptName = table.script_name || 'Unknown';
        return [scriptName, tableName];
    });
    createFilteredNetworkVisualization(tablePairs);
}

function createFilteredNetworkVisualization(selectedTablePairs) {
    // selectedTablePairs: array of [script_name, table_name]
    const container = document.getElementById('networkContainer');
    // Deduplicate by script_name + table_name
    const uniqueTableKeys = new Set(selectedTablePairs.map(pair => pair[0] + '::' + pair[1]));
    // Create nodes
    const nodes = [];
    uniqueTableKeys.forEach(key => {
        const table = tableKeyMap[key];
        if (table) {
            const tableName = key.split('::')[1];
            nodes.push({
                id: key,
                label: tableName,
                title: `${tableName}\nSources: ${table.source.length}\nTargets: ${table.target.length}\nVolatile: ${table.is_volatile ? 'Yes' : 'No'}`,
                color: getNodeColor(table),
                size: 20 + Math.min((table.source?.length || 0) + (table.target?.length || 0), 10) * 2,
                font: { size: 12, face: 'Arial' }
            });
        }
    });
    // Create edges
    const edges = [];
    const edgeMap = new Map(); // To avoid duplicate edges
    
    uniqueTableKeys.forEach(key => {
        const table = tableKeyMap[key];
        if (table) {
            // Source relationships
            (table.source || []).forEach(rel => {
                // Find the related table in the selected set by checking all possible script combinations
                let foundSrcKey = null;
                for (const selectedKey of uniqueTableKeys) {
                    const selectedTableName = selectedKey.split('::')[1];
                    if (selectedTableName === rel.name) {
                        foundSrcKey = selectedKey;
                        break;
                    }
                }
                
                if (foundSrcKey) {
                    const edgeKey = `${foundSrcKey}->${key}`;
                    if (!edgeMap.has(edgeKey)) {
                        edgeMap.set(edgeKey, true);
                        const opLabel = getOperationDisplayText([{ scriptName: table.script_name || 'Unknown', indices: rel.operation }]);
                        edges.push({
                            from: foundSrcKey,
                            to: key,
                            arrows: 'to',
                            color: { color: '#28a745', opacity: 0.8 },
                            width: 2,
                            title: `Operations: ${opLabel}`,
                            label: opLabel,
                            font: { size: 10, color: '#28a745' }
                        });
                    }
                }
            });
            
            // Target relationships
            (table.target || []).forEach(rel => {
                // Find the related table in the selected set by checking all possible script combinations
                let foundTgtKey = null;
                for (const selectedKey of uniqueTableKeys) {
                    const selectedTableName = selectedKey.split('::')[1];
                    if (selectedTableName === rel.name) {
                        foundTgtKey = selectedKey;
                        break;
                    }
                }
                
                if (foundTgtKey) {
                    const edgeKey = `${key}->${foundTgtKey}`;
                    if (!edgeMap.has(edgeKey)) {
                        edgeMap.set(edgeKey, true);
                        const opLabel = getOperationDisplayText([{ scriptName: table.script_name || 'Unknown', indices: rel.operation }]);
                        edges.push({
                            from: key,
                            to: foundTgtKey,
                            arrows: 'to',
                            color: { color: '#dc3545', opacity: 0.8 },
                            width: 2,
                            title: `Operations: ${opLabel}`,
                            label: opLabel,
                            font: { size: 10, color: '#dc3545' }
                        });
                    }
                }
            });
        }
    });
    // Render network
    const data = {
        nodes: new vis.DataSet(nodes),
        edges: new vis.DataSet(edges)
    };
    const options = {
        nodes: { shape: 'box', borderWidth: 2, shadow: true },
        edges: { smooth: { type: 'curvedCW', roundness: 0.2 } },
        physics: { enabled: false },
        interaction: { hover: true, tooltipDelay: 200, zoomView: true, dragView: true },
        layout: { improvedLayout: true }
    };
    if (network) network.destroy();
    network = new vis.Network(container, data, options);
    network.on('click', function(params) {
        if (params.nodes.length > 0) {
            // Node clicked - apply filter to show this table and its relationships
            const clickedNodeId = params.nodes[0];
            showDirectlyRelatedNodes(clickedNodeId);
            hideSidePanel();
        } else if (params.edges.length > 0) {
            // Edge clicked
            showEdgeDetails(params.edges[0]);
        } else {
            hideSidePanel();
        }
    });
}

function hideSidePanel() {
    const sidePanel = document.getElementById('networkSidePanel');
    sidePanel.style.display = 'none';
}

function initializeResizeHandles() {
    const resizeHandles = document.querySelectorAll('.resize-handle');
    
    resizeHandles.forEach(handle => {
        let isDragging = false;
        let startX, startWidth;
        
        handle.addEventListener('mousedown', function(e) {
            isDragging = true;
            startX = e.clientX;
            
            // Get the sidebar element (previous sibling of the handle)
            const sidebar = handle.previousElementSibling;
            startWidth = sidebar.offsetWidth;
            
            handle.classList.add('dragging');
            document.body.style.cursor = 'col-resize';
            document.body.style.userSelect = 'none';
            
            e.preventDefault();
        });
        
        document.addEventListener('mousemove', function(e) {
            if (!isDragging) return;
            
            const deltaX = e.clientX - startX;
            const sidebar = handle.previousElementSibling;
            const newWidth = Math.max(200, Math.min(600, startWidth + deltaX));
            
            sidebar.style.width = newWidth + 'px';
            handle.style.left = newWidth + 'px';
        });
        
        document.addEventListener('mouseup', function() {
            if (isDragging) {
                isDragging = false;
                handle.classList.remove('dragging');
                document.body.style.cursor = '';
                document.body.style.userSelect = '';
            }
        });
    });
}

// Close modal when clicking outside
window.onclick = function(event) {
    const sqlModal = document.getElementById('sqlModal');
    
    if (event.target === sqlModal) {
        closeSqlModal();
    }
}

// Add deselection functionality
document.addEventListener('click', function(event) {
    // Deselect when clicking on empty areas in content areas
    const contentArea = document.getElementById('contentArea');
    const statementContentArea = document.getElementById('statementContentArea');
    
    if (contentArea && contentArea.contains(event.target) && 
        !event.target.closest('.table-item, .tree-item, .statement-item, .relationship-item, .operation-badge')) {
        deselectAll();
    }
    
    if (statementContentArea && statementContentArea.contains(event.target) && 
        !event.target.closest('.table-item, .tree-item, .statement-item, .relationship-item, .operation-badge')) {
        deselectAll();
    }
});

// Add keyboard shortcuts for deselection
document.addEventListener('keydown', function(event) {
    // Escape key to deselect all
    if (event.key === 'Escape') {
        deselectAll();
    }
});

// Search/filter for network node by name
let allTableNames = [];
let filteredTableNames = [];
let selectedAutocompleteIndex = -1;

function initializeTableNames() {
    if (lineageData && lineageData.tables) {
        allTableNames = Object.keys(lineageData.tables).sort();
    }
}



function searchNetworkNode() {
    const input = document.getElementById('networkNodeSearchInput');
    const query = input.value.trim().toLowerCase();
    if (!query) {
        if (selectedNetworkScript && lineageData.file_groups[selectedNetworkScript]) {
            const tables = lineageData.file_groups[selectedNetworkScript].tables;
            const tablePairs = tables.map(tableName => {
                const table = lineageData.tables[tableName];
                const scriptName = table.script_name || 'Unknown';
                return [scriptName, tableName];
            });
            createFilteredNetworkVisualization(tablePairs);
        } else {
            showAllNodes();
        }
        return;
    }
    // Find all nodes that match (case-insensitive, partial match)
    let searchScope = allTableNames;
    if (selectedNetworkScript && lineageData.file_groups[selectedNetworkScript]) {
        searchScope = lineageData.file_groups[selectedNetworkScript].tables;
    }
    const matchedNodes = searchScope.filter(tableName => tableName.toLowerCase().includes(query));
    if (matchedNodes.length === 1) {
        showDirectlyRelatedNodes(matchedNodes[0]);
        hideAutocompleteDropdown();
    } else if (matchedNodes.length > 1) {
        const tablePairs = matchedNodes.map(tableName => {
            const table = lineageData.tables[tableName];
            const scriptName = table.script_name || 'Unknown';
            return [scriptName, tableName];
        });
        createFilteredNetworkVisualization(tablePairs);
        hideAutocompleteDropdown();
    } else {
        // Show empty network with a message
        const container = document.getElementById('networkContainer');
        container.innerHTML = '<div style="padding: 40px; color: #dc3545; text-align: center; font-size: 1.2em;">No table found matching that name.</div>';
        if (window.network) {
            window.network.destroy();
            window.network = null;
        }
    }
}

function clearNetworkNodeSearch() {
    document.getElementById('networkNodeSearchInput').value = '';
    if (selectedNetworkScript && lineageData.file_groups[selectedNetworkScript]) {
        const tables = lineageData.file_groups[selectedNetworkScript].tables;
        const tablePairs = tables.map(tableName => {
            const table = lineageData.tables[tableName];
            const scriptName = table.script_name || 'Unknown';
            return [scriptName, tableName];
        });
        createFilteredNetworkVisualization(tablePairs);
    } else {
        showAllNodes();
    }
}

function showAutocompleteDropdown() {
    const dropdown = document.getElementById('autocompleteDropdown');
    if (filteredTableNames.length > 0) {
        dropdown.style.display = 'block';
    } else {
        dropdown.style.display = 'none';
    }
}

function hideAutocompleteDropdown() {
    const dropdown = document.getElementById('autocompleteDropdown');
    dropdown.style.display = 'none';
    selectedAutocompleteIndex = -1;
}

function updateAutocompleteDropdown() {
    const input = document.getElementById('networkNodeSearchInput');
    const query = input.value.trim().toLowerCase();
    const dropdown = document.getElementById('autocompleteDropdown');
    
    if (!query) {
        hideAutocompleteDropdown();
        return;
    }
    
    // Filter table names based on query (case-insensitive substring match)
    filteredTableNames = allTableNames.filter(tableName => 
        tableName.toLowerCase().includes(query)
    );
    
    if (filteredTableNames.length > 0) {
        // Create dropdown items
        dropdown.innerHTML = filteredTableNames.map((tableName, index) => 
            `<div class="autocomplete-item" onclick="selectAutocompleteItem(${index})">${tableName}</div>`
        ).join('');
        showAutocompleteDropdown();
    } else {
        hideAutocompleteDropdown();
    }
}

function selectAutocompleteItem(index) {
    if (index >= 0 && index < filteredTableNames.length) {
        const selectedTable = filteredTableNames[index];
        document.getElementById('networkNodeSearchInput').value = selectedTable;
        hideAutocompleteDropdown();
        showDirectlyRelatedNodes(selectedTable);
    }
}

function navigateAutocomplete(direction) {
    const items = document.querySelectorAll('.autocomplete-item');
    if (items.length === 0) return;
    
    if (direction === 'up') {
        selectedAutocompleteIndex = selectedAutocompleteIndex <= 0 ? items.length - 1 : selectedAutocompleteIndex - 1;
    } else if (direction === 'down') {
        selectedAutocompleteIndex = selectedAutocompleteIndex >= items.length - 1 ? 0 : selectedAutocompleteIndex + 1;
    }
    
    // Update visual selection
    items.forEach((item, index) => {
        item.classList.toggle('selected', index === selectedAutocompleteIndex);
    });
}

// Allow pressing Enter in the search input to trigger search
document.addEventListener('DOMContentLoaded', function() {
    const input = document.getElementById('networkNodeSearchInput');
    if (input) {
        input.addEventListener('input', function() {
            updateAutocompleteDropdown();
        });
        
        input.addEventListener('keydown', function(e) {
            if (e.key === 'Enter') {
                if (selectedAutocompleteIndex >= 0) {
                    selectAutocompleteItem(selectedAutocompleteIndex);
                } else {
                    searchNetworkNode();
                }
                e.preventDefault();
            } else if (e.key === 'ArrowUp') {
                navigateAutocomplete('up');
                e.preventDefault();
            } else if (e.key === 'ArrowDown') {
                navigateAutocomplete('down');
                e.preventDefault();
            } else if (e.key === 'Escape') {
                hideAutocompleteDropdown();
                e.preventDefault();
            }
        });
        
        // Hide dropdown when clicking outside
        document.addEventListener('click', function(e) {
            if (!e.target.closest('.autocomplete-container')) {
                hideAutocompleteDropdown();
            }
        });
    }
});

function loadAllLineageFilesFromFolder(folderPath) {
    // Load the all_lineage.txt file from the specified folder and process all JSON files listed in it
    fetch(`${folderPath}/all_lineage.txt`)
        .then(response => {
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            return response.text();
        })
        .then(text => {
            const jsonFiles = text.trim().split('\n').filter(line => line.length > 0);
            console.log(`Found ${jsonFiles.length} JSON files in ${folderPath}/all_lineage.txt`);
            
            if (jsonFiles.length === 0) {
                showError('No JSON files found in all_lineage.txt');
                return;
            }
            
            // Initialize merged data structure
            const mergedData = {
                tables: {},
                bteq_statements: [],
                source_file: `${jsonFiles.length}`,
                file_groups: {}
            };
            
            let processedFiles = 0;
            const totalFiles = jsonFiles.length;
            
            // Process each JSON file
            jsonFiles.forEach((jsonFile, index) => {
                // Use the JSON file path relative to the folder
                fetch(`${folderPath}/${jsonFile}`)
                    .then(response => {
                        if (!response.ok) {
                            throw new Error(`HTTP error! status: ${response.status}`);
                        }
                        return response.json();
                    })
                    .then(data => {
                        console.log(`Processing file: ${jsonFile}`);
                        
                        // Create file group using script_name from the data
                        const scriptName = data.script_name || jsonFile.replace('_lineage.json', '');
                        mergedData.file_groups[scriptName] = {
                            tables: [],
                            statements: []
                        };
                        
                        // Merge tables with file tracking
                        if (data.tables) {
                            Object.keys(data.tables).forEach(tableName => {
                                mergedData.tables[tableName] = data.tables[tableName];
                                // Add script_name to the table object
                                mergedData.tables[tableName].script_name = scriptName;
                                mergedData.file_groups[scriptName].tables.push(tableName);
                            });
                        }
                        
                        // Merge BTEQ statements with file tracking
                        if (data.bteq_statements) {
                            const startIndex = mergedData.bteq_statements.length;
                            mergedData.bteq_statements.push(...data.bteq_statements);
                            
                            // Track statement indices for this file
                            for (let i = 0; i < data.bteq_statements.length; i++) {
                                mergedData.file_groups[scriptName].statements.push(startIndex + i);
                            }
                        }
                        
                        processedFiles++;
                        
                        // When all files are processed, update the display
                        if (processedFiles === totalFiles) {
                                                        console.log(`Successfully merged ${totalFiles} files from ${folderPath}`);
                    lineageData = mergedData;
                    buildScriptAwareMappings();
                    displaySummary();
                    displayTables();
                    displayStatements();
                    displayNetworkFileGroups();
                    document.getElementById('summarySection').style.display = 'block';
                    document.getElementById('tabSection').style.display = 'block';
                    initializeTableNames();
                            
                            // Update URL to include the folder path
                            const url = new URL(window.location);
                            url.searchParams.set('folder', folderPath);
                            window.history.pushState({}, '', url);
                        }
                    })
                    .catch(error => {
                        console.error(`Error processing JSON file ${jsonFile}:`, error);
                        processedFiles++;
                        
                        if (processedFiles === totalFiles) {
                            showError(`Error processing some files from ${folderPath}. Check console for details.`);
                        }
                    });
            });
        })
        .catch(error => {
            showError(`Error loading all_lineage.txt from ${folderPath}: ${error.message}`);
        });
}

// Add a div above the networkContainer to display the selected script name
function updateSelectedScriptLabel() {
    const labelDiv = document.getElementById('selectedScriptLabel');
    if (!labelDiv) return;
    if (selectedNetworkScript && lineageData.file_groups && lineageData.file_groups[selectedNetworkScript]) {

        const scriptName = selectedNetworkScript;
        labelDiv.textContent = `Script: ${scriptName}`;
        labelDiv.style.display = '';
    } else {
        labelDiv.textContent = 'All Scripts';
        labelDiv.style.display = '';
    }
}