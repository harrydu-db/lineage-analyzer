let lineageData = null;
let selectedTable = null;
let network = null;

// Track the currently selected script in the network view
let selectedNetworkScript = null;
// Track the currently selected table filters in the network view
let selectedTableFilters = [];
// Track the connection mode: 'direct', 'impacts_by', 'impacted_by', or 'both'
let connectionMode = 'direct';
// Track the lock view state to prevent accidental filter changes
let lockViewEnabled = false;
// Track the last applied filters to avoid unnecessary network recreation
let lastNetworkFilters = { scriptFilters: [], tableFilters: [], mode: 'direct' };
// Track the flow view state for hierarchical layout
let flowViewEnabled = false;

// Global data structures for proper ownership modeling
let allNodes = {};
let allEdges = [];

// Autocomplete variables for table search
let allTableNames = [];
let filteredTableNames = [];
let selectedAutocompleteIndex = -1;

// Autocomplete variables for script search
let allScriptNames = [];
let filteredScriptNames = [];
let selectedScriptAutocompleteIndex = -1;

// Function to reset all network-related data when new data is loaded
function resetNetworkData() {
    console.log('Resetting network data for new data load...');
    
    // Reset network visualization
    if (network) {
        network.destroy();
        network = null;
    }
    
    // Reset network filters and selections
    selectedNetworkScript = null;
    selectedTableFilters = [];
    connectionMode = 'direct';
    lockViewEnabled = false;
    flowViewEnabled = false;
    lastNetworkFilters = { scriptFilters: [], tableFilters: [], mode: 'direct' };
    
    // Reset ownership model data
    allNodes = {};
    allEdges = [];
    
    // Reset autocomplete data
    allTableNames = [];
    filteredTableNames = [];
    selectedAutocompleteIndex = -1;
    
    allScriptNames = [];
    filteredScriptNames = [];
    selectedScriptAutocompleteIndex = -1;
    
    // Reset table selection
    selectedTable = null;
    
    // Clear any existing network container content
    const networkContainer = document.getElementById('networkContainer');
    if (networkContainer) {
        networkContainer.innerHTML = '';
    }
    
    // Clear selected script label
    const selectedScriptLabel = document.getElementById('selectedScriptLabel');
    if (selectedScriptLabel) {
        selectedScriptLabel.textContent = 'Filter: None';
    }
    
    // Clear input fields
    const scriptSearchInput = document.getElementById('networkScriptSearchInput');
    if (scriptSearchInput) {
        scriptSearchInput.value = '';
    }
    
    const tableSearchInput = document.getElementById('networkNodeSearchInput');
    if (tableSearchInput) {
        tableSearchInput.value = '';
    }
    
    // Reset flow view checkbox
    const flowViewCheckbox = document.getElementById('flowViewCheckbox');
    if (flowViewCheckbox) {
        flowViewCheckbox.checked = false;
    }
    
    // Hide any open dropdowns
    hideAutocompleteDropdown();
    hideScriptAutocompleteDropdown();
    
    console.log('Network data reset complete');
}

// Build proper ownership-based data model
function buildOwnershipModel() {
    allNodes = {};
    allEdges = [];
    // Reset filter tracking when new data is loaded
    lastNetworkFilters = { scriptFilters: [], tableFilters: [], mode: 'direct' };
    
    if (!lineageData || !lineageData.scripts) return;
    

    
    console.log('Building ownership model...');
    console.log('Total scripts to process:', Object.keys(lineageData.scripts).length);
    
    // PASS 1: Build all nodes with proper ownership
    console.log('=== PASS 1: Building nodes ===');
    
    // First, create nodes for all defined tables from each script
    for (const [scriptName, scriptData] of Object.entries(lineageData.scripts)) {
        console.log(`Processing script: ${scriptName}`);
        
        for (const [tableName, tableObj] of Object.entries(scriptData.tables || {})) {
            // Determine node ID based on volatility
            let nodeId;
            if (tableObj.is_volatile) {
                // Volatile tables are script-specific
                nodeId = `${scriptName}::${tableName}`;
            } else {
                // Non-volatile tables are global
                nodeId = tableName;
            }
            
            // Create or update node
            if (allNodes[nodeId]) {
                // Node already exists (global table used by multiple scripts)
                if (!allNodes[nodeId].owners.includes(scriptName)) {
                    allNodes[nodeId].owners.push(scriptName);
                }
            } else {
                // Create new node
                allNodes[nodeId] = {
                    id: nodeId,
                    name: tableName,
                    is_volatile: tableObj.is_volatile,
                    owners: [scriptName],
                    source: tableObj.source || [],
                    target: tableObj.target || [],
                    properties: { ...tableObj, script_name: scriptName }
                };
            }
            
            console.log(`Created/updated node: ${nodeId} (${tableObj.is_volatile ? 'volatile' : 'global'})`);
        }
    }
    
    // Second, ensure all referenced tables (that might not be defined) are also included
    for (const [scriptName, scriptData] of Object.entries(lineageData.scripts)) {
        for (const [tableName, tableObj] of Object.entries(scriptData.tables || {})) {
            // Process source relationships to ensure referenced tables exist as nodes
            if (tableObj.source) {
                tableObj.source.forEach(rel => {
                    // Find the source table - prioritize same script first
                    let sourceTable = null;
                    let sourceScript = null;
                    
                    // First, try to find the table in the current script
                    if (scriptData.tables && scriptData.tables[rel.name]) {
                        sourceTable = scriptData.tables[rel.name];
                        sourceScript = scriptName;
                    } else {
                        // If not found in current script, look in other scripts
                        for (const [sName, sData] of Object.entries(lineageData.scripts)) {
                            if (sData.tables && sData.tables[rel.name]) {
                                sourceTable = sData.tables[rel.name];
                                sourceScript = sName;
                                break;
                            }
                        }
                    }
                    
                    // Determine source node ID based on the source table's properties
                    let sourceNodeId;
                    if (sourceTable && sourceTable.is_volatile) {
                        // Source is volatile, needs script prefix
                        sourceNodeId = `${sourceScript}::${rel.name}`;
                    } else {
                        // Source is global (or doesn't exist in our data)
                        sourceNodeId = rel.name;
                    }
                    
                    // Create node for referenced table if it doesn't exist
                    if (!allNodes[sourceNodeId]) {
                        allNodes[sourceNodeId] = {
                            id: sourceNodeId,
                            name: rel.name,
                            is_volatile: sourceTable ? sourceTable.is_volatile : false,
                            owners: sourceTable ? [sourceScript] : [],
                            source: sourceTable ? (sourceTable.source || []) : [],
                            target: sourceTable ? (sourceTable.target || []) : [],
                            properties: sourceTable ? { ...sourceTable, script_name: sourceScript } : { name: rel.name }
                        };
                        console.log(`Created referenced node: ${sourceNodeId} (${sourceTable ? (sourceTable.is_volatile ? 'volatile' : 'global') : 'external'})`);
                    }
                });
            }
            
            // Process target relationships to ensure referenced tables exist as nodes
            if (tableObj.target) {
                tableObj.target.forEach(rel => {
                    // Find the target table - prioritize same script first
                    let targetTable = null;
                    let targetScript = null;
                    
                    // First, try to find the table in the current script
                    if (scriptData.tables && scriptData.tables[rel.name]) {
                        targetTable = scriptData.tables[rel.name];
                        targetScript = scriptName;
                    } else {
                        // If not found in current script, look in other scripts
                        for (const [sName, sData] of Object.entries(lineageData.scripts)) {
                            if (sData.tables && sData.tables[rel.name]) {
                                targetTable = sData.tables[rel.name];
                                targetScript = sName;
                                break;
                            }
                        }
                    }
                    
                    let targetNodeId;
                    if (targetTable && targetTable.is_volatile) {
                        // Target is volatile, needs script prefix
                        targetNodeId = `${targetScript}::${rel.name}`;
                    } else {
                        // Target is global (or doesn't exist in our data)
                        targetNodeId = rel.name;
                    }
                    
                    // Create node for referenced table if it doesn't exist
                    if (!allNodes[targetNodeId]) {
                        allNodes[targetNodeId] = {
                            id: targetNodeId,
                            name: rel.name,
                            is_volatile: targetTable ? targetTable.is_volatile : false,
                            owners: targetTable ? [targetScript] : [],
                            source: targetTable ? (targetTable.source || []) : [],
                            target: targetTable ? (targetTable.target || []) : [],
                            properties: targetTable ? { ...targetTable, script_name: targetScript } : { name: rel.name }
                        };
                        console.log(`Created referenced node: ${targetNodeId} (${targetTable ? (targetTable.is_volatile ? 'volatile' : 'global') : 'external'})`);
                    }
                });
            }
        }
    }
    
    console.log(`PASS 1 complete: ${Object.keys(allNodes).length} nodes created`);
    
    // PASS 2: Build all edges using complete node information
    console.log('=== PASS 2: Building edges ===');
    
    // Create edge map for O(1) lookup performance
    const edgeMap = new Map();
    
    for (const [scriptName, scriptData] of Object.entries(lineageData.scripts)) {
        for (const [tableName, tableObj] of Object.entries(scriptData.tables || {})) {
            // Determine current node ID
            let currentNodeId;
            if (tableObj.is_volatile) {
                currentNodeId = `${scriptName}::${tableName}`;
            } else {
                currentNodeId = tableName;
            }
            
            // Process source relationships (tables that provide data to this table)
            if (tableObj.source) {
                tableObj.source.forEach(rel => {
                    // Find the source table - prioritize same script first
                    let sourceTable = null;
                    let sourceScript = null;
                    
                    // First, try to find the table in the current script
                    if (scriptData.tables && scriptData.tables[rel.name]) {
                        sourceTable = scriptData.tables[rel.name];
                        sourceScript = scriptName;
                    } else {
                        // If not found in current script, look in other scripts
                        for (const [sName, sData] of Object.entries(lineageData.scripts)) {
                            if (sData.tables && sData.tables[rel.name]) {
                                sourceTable = sData.tables[rel.name];
                                sourceScript = sName;
                                break;
                            }
                        }
                    }
                    
                    // Determine source node ID based on the source table's properties
                    let sourceNodeId;
                    if (sourceTable && sourceTable.is_volatile) {
                        // Source is volatile, needs script prefix
                        sourceNodeId = `${sourceScript}::${rel.name}`;
                    } else {
                        // Source is global (or doesn't exist in our data)
                        sourceNodeId = rel.name;
                    }
                    
                    // Only create edge if both nodes exist (allow self-references for UPDATE operations)
                    if (allNodes[sourceNodeId] && allNodes[currentNodeId]) {
                        // Create edge
                        const edgeKey = `${sourceNodeId}->${currentNodeId}`;
                        const existingEdge = edgeMap.get(edgeKey);
                        
                        if (existingEdge) {
                            // Edge already exists, add operations if this script has operations for this relationship
                            if (rel.operation && rel.operation.length > 0) {
                                const existingOps = new Set(existingEdge[2]);
                                rel.operation.forEach(opIndex => {
                                    existingOps.add(`${scriptName}::op${opIndex}`);
                                });
                                existingEdge[2] = Array.from(existingOps);
                                console.log(`Updated existing edge: ${sourceNodeId} -> ${currentNodeId} with operations from ${scriptName}`);
                            } else {
                                console.warn(`Skipping operation update for existing edge: ${sourceNodeId} -> ${currentNodeId} (no operations defined in relationship)`);
                            }
                        } else {
                            // Create new edge - if there are operations and both nodes exist
                            if (rel.operation && rel.operation.length > 0) {
                                const operations = rel.operation.map(opIndex => `${scriptName}::op${opIndex}`);
                                const newEdge = [sourceNodeId, currentNodeId, operations];
                                allEdges.push(newEdge);
                                edgeMap.set(edgeKey, newEdge);
                                console.log(`Created edge: ${sourceNodeId} -> ${currentNodeId} (${operations.length} operations) from script ${scriptName}`);
                            } else {
                                // Skip self-loops without operations
                                if (sourceNodeId === currentNodeId) {
                                    console.warn(`Skipping self-loop edge: ${sourceNodeId} -> ${currentNodeId} (no operations defined) - Relationship: ${JSON.stringify(rel)}`);
                                } else {
                                    console.warn(`Skipping edge: ${sourceNodeId} -> ${currentNodeId} (no operations defined) - Relationship: ${JSON.stringify(rel)}`);
                                }
                            }
                        }
                    } else {
                        console.warn(`Skipping edge: ${sourceNodeId} -> ${currentNodeId} (one or both nodes don't exist)`);
                    }
                });
            }
            
            // Process target relationships (tables that receive data from this table)
            if (tableObj.target) {
                tableObj.target.forEach(rel => {
                    // Find the target table - prioritize same script first
                    let targetTable = null;
                    let targetScript = null;
                    
                    // First, try to find the table in the current script
                    if (scriptData.tables && scriptData.tables[rel.name]) {
                        targetTable = scriptData.tables[rel.name];
                        targetScript = scriptName;
                    } else {
                        // If not found in current script, look in other scripts
                        for (const [sName, sData] of Object.entries(lineageData.scripts)) {
                            if (sData.tables && sData.tables[rel.name]) {
                                targetTable = sData.tables[rel.name];
                                targetScript = sName;
                                break;
                            }
                        }
                    }
                    
                    // Determine target node ID based on the target table's properties
                    let targetNodeId;
                    if (targetTable && targetTable.is_volatile) {
                        // Target is volatile, needs script prefix
                        targetNodeId = `${targetScript}::${rel.name}`;
                    } else {
                        // Target is global (or doesn't exist in our data)
                        targetNodeId = rel.name;
                    }
                    
                    // Only create edge if both nodes exist (allow self-references for UPDATE operations)
                    if (allNodes[currentNodeId] && allNodes[targetNodeId]) {
                        // Create edge
                        const edgeKey = `${currentNodeId}->${targetNodeId}`;
                        const existingEdge = edgeMap.get(edgeKey);
                        
                        if (existingEdge) {
                            // Edge already exists, add operations if this script has operations for this relationship
                            if (rel.operation && rel.operation.length > 0) {
                                const existingOps = new Set(existingEdge[2]);
                                rel.operation.forEach(opIndex => {
                                    existingOps.add(`${scriptName}::op${opIndex}`);
                                });
                                existingEdge[2] = Array.from(existingOps);
                                console.log(`Updated existing edge: ${currentNodeId} -> ${targetNodeId} with operations from ${scriptName}`);
                            } else {
                                console.warn(`Skipping operation update for existing edge: ${currentNodeId} -> ${targetNodeId} (no operations defined in relationship)`);
                            }
                        } else {
                            // Create new edge - if there are operations and both nodes exist
                            if (rel.operation && rel.operation.length > 0) {
                                const operations = rel.operation.map(opIndex => `${scriptName}::op${opIndex}`);
                                const newEdge = [currentNodeId, targetNodeId, operations];
                                allEdges.push(newEdge);
                                edgeMap.set(edgeKey, newEdge);
                                console.log(`Created edge: ${currentNodeId} -> ${targetNodeId} (${operations.length} operations) from script ${scriptName}`);
                            } else {
                                // Skip self-loops without operations
                                if (currentNodeId === targetNodeId) {
                                    console.warn(`Skipping self-loop edge: ${currentNodeId} -> ${targetNodeId} (no operations defined) - Relationship: ${JSON.stringify(rel)}`);
                                } else {
                                    console.warn(`Skipping edge: ${currentNodeId} -> ${targetNodeId} (no operations defined) - Relationship: ${JSON.stringify(rel)}`);
                                }
                            }
                        }
                    } else {
                        console.warn(`Skipping edge: ${currentNodeId} -> ${targetNodeId} (one or both nodes don't exist)`);
                    }
                });
            }
        }
    }
    
    console.log(`PASS 2 complete: ${allEdges.length} edges created`);
    
    // PASS 3: Discover all script owners for each table
    console.log('=== PASS 3: Discovering all owners ===');
    
    // Create a map to track all scripts that reference each table
    const tableOwners = {};
    
    // First pass: Initialize ownership for all tables
    for (const [scriptName, scriptData] of Object.entries(lineageData.scripts)) {
        for (const [tableName, tableObj] of Object.entries(scriptData.tables || {})) {
            // For volatile tables, track ownership by the full node ID
            // For global tables, track by table name
            const ownershipKey = tableObj.is_volatile ? `${scriptName}::${tableName}` : tableName;
            
            if (!tableOwners[ownershipKey]) {
                tableOwners[ownershipKey] = new Set();
            }
            
            // For volatile tables: only the creating script is the owner
            if (tableObj.is_volatile) {
                tableOwners[ownershipKey].add(scriptName);
            } else {
                // For global tables: the script that defines the table is an owner
                tableOwners[ownershipKey].add(scriptName);
            }
        }
    }
    
    // Second pass: Find all scripts that reference each table through relationships
    for (const [scriptName, scriptData] of Object.entries(lineageData.scripts)) {
        for (const [tableName, tableObj] of Object.entries(scriptData.tables || {})) {
            // Scan source relationships to find tables that are referenced
            if (tableObj.source) {
                tableObj.source.forEach(rel => {
                    // Find the source table - prioritize same script first
                    let sourceTable = null;
                    let sourceScript = null;
                    
                    // First, try to find the table in the current script
                    if (scriptData.tables && scriptData.tables[rel.name]) {
                        sourceTable = scriptData.tables[rel.name];
                        sourceScript = scriptName;
                    } else {
                        // If not found in current script, look in other scripts
                        for (const [sName, sData] of Object.entries(lineageData.scripts)) {
                            if (sData.tables && sData.tables[rel.name]) {
                                sourceTable = sData.tables[rel.name];
                                sourceScript = sName;
                                break;
                            }
                        }
                    }
                    
                    if (sourceTable && !sourceTable.is_volatile) {
                        // Any script that references a global table becomes an owner
                        if (!tableOwners[rel.name]) {
                            tableOwners[rel.name] = new Set();
                        }
                        tableOwners[rel.name].add(scriptName);
                    }
                });
            }
            
            // Scan target relationships to find tables that are referenced
            if (tableObj.target) {
                tableObj.target.forEach(rel => {
                    // Find the target table - prioritize same script first
                    let targetTable = null;
                    let targetScript = null;
                    
                    // First, try to find the table in the current script
                    if (scriptData.tables && scriptData.tables[rel.name]) {
                        targetTable = scriptData.tables[rel.name];
                        targetScript = scriptName;
                    } else {
                        // If not found in current script, look in other scripts
                        for (const [sName, sData] of Object.entries(lineageData.scripts)) {
                            if (sData.tables && sData.tables[rel.name]) {
                                targetTable = sData.tables[rel.name];
                                targetScript = sName;
                                break;
                            }
                        }
                    }
                    
                    if (targetTable && !targetTable.is_volatile) {
                        // Any script that references a global table becomes an owner
                        if (!tableOwners[rel.name]) {
                            tableOwners[rel.name] = new Set();
                        }
                        tableOwners[rel.name].add(scriptName);
                    }
                });
            }
        }
    }
    
    // Third pass: Process all nodes to ensure we capture all relationships
    // This handles cases where tables are referenced but not defined in lineageData.tables
    for (const [nodeId, node] of Object.entries(allNodes)) {
        const tableName = node.name;
        
        // For volatile tables, we need to track ownership by the full node ID
        // For global tables, we track by table name
        const ownershipKey = node.is_volatile ? nodeId : tableName;
        
        // Initialize tableOwners for this ownership key if not exists
        if (!tableOwners[ownershipKey]) {
            tableOwners[ownershipKey] = new Set();
        }
        
        // Add the defining script as owner (if this node was created from a defined table)
        if (node.properties && node.properties.script_name) {
            tableOwners[ownershipKey].add(node.properties.script_name);
        }
        
        // Process source relationships from the node
        if (node.source) {
            node.source.forEach(rel => {
                // Find the source table in any script
                let sourceTable = null;
                if (lineageData.scripts) {
                    for (const [sName, sData] of Object.entries(lineageData.scripts)) {
                        if (sData.tables && sData.tables[rel.name]) {
                            sourceTable = sData.tables[rel.name];
                            break;
                        }
                    }
                } else {
                    // Fallback for legacy single file mode
                    sourceTable = lineageData.tables && lineageData.tables[rel.name];
                }
                
                if (sourceTable && !sourceTable.is_volatile) {
                    // Any script that references a global table becomes an owner
                    if (!tableOwners[rel.name]) {
                        tableOwners[rel.name] = new Set();
                    }
                    // Add the script that owns this node as an owner of the source table
                    if (node.properties && node.properties.script_name) {
                        tableOwners[rel.name].add(node.properties.script_name);
                    }
                }
            });
        }
        
        // Process target relationships from the node
        if (node.target) {
            node.target.forEach(rel => {
                // Find the target table in any script
                let targetTable = null;
                if (lineageData.scripts) {
                    for (const [sName, sData] of Object.entries(lineageData.scripts)) {
                        if (sData.tables && sData.tables[rel.name]) {
                            targetTable = sData.tables[rel.name];
                            break;
                        }
                    }
                } else {
                    // Fallback for legacy single file mode
                    targetTable = lineageData.tables && lineageData.tables[rel.name];
                }
                
                if (targetTable && !targetTable.is_volatile) {
                    // Any script that references a global table becomes an owner
                    if (!tableOwners[rel.name]) {
                        tableOwners[rel.name] = new Set();
                    }
                    // Add the script that owns this node as an owner of the target table
                    if (node.properties && node.properties.script_name) {
                        tableOwners[rel.name].add(node.properties.script_name);
                    }
                }
            });
        }
    }
    
    // Update all nodes with complete ownership information
    Object.entries(allNodes).forEach(([nodeId, node]) => {
        const tableName = node.name;
        
        // For volatile tables, look up ownership by the full node ID
        // For global tables, look up by table name
        const ownershipKey = node.is_volatile ? nodeId : tableName;
        
        if (tableOwners[ownershipKey]) {
            // Convert Set to Array and sort for consistency
            const allOwners = Array.from(tableOwners[ownershipKey]).sort();
            node.owners = allOwners;
            console.log(`Updated ${nodeId}: owners = [${allOwners.join(', ')}]`);
            
            // Validate volatile table ownership
            if (node.is_volatile && allOwners.length > 1) {
                console.error(`❌ VIOLATION: Volatile table ${nodeId} has multiple owners: [${allOwners.join(', ')}]`);
                console.error(`   Volatile tables should only have one owner (the creating script)`);
                console.error(`   Keeping only the first owner: ${allOwners[0]}`);
                // Fix the violation by keeping only the first owner
                node.owners = [allOwners[0]];
            } else if (node.is_volatile && allOwners.length === 1) {
                console.log(`✅ Volatile table ${nodeId} correctly has single owner: ${allOwners[0]}`);
            }
        }
    });
    
    console.log(`PASS 3 complete: Updated ownership for ${Object.keys(allNodes).length} nodes`);
    
    // Final validation: ensure all volatile tables have exactly one owner
    console.log('=== FINAL VALIDATION ===');
    let finalViolations = 0;
    Object.entries(allNodes).forEach(([nodeId, node]) => {
        if (node.is_volatile) {
            if (node.owners.length !== 1) {
                console.error(`❌ FINAL VIOLATION: Volatile table ${nodeId} has ${node.owners.length} owners: [${node.owners.join(', ')}]`);
                finalViolations++;
            } else {
                console.log(`✅ Volatile table ${nodeId} has correct single owner: ${node.owners[0]}`);
            }
        }
    });
    
    if (finalViolations === 0) {
        console.log('✅ All volatile tables have exactly one owner');
    } else {
        console.error(`❌ ${finalViolations} volatile table(s) still have incorrect ownership`);
    }
    
    // Validation and summary
    console.log('=== OWNERSHIP MODEL SUMMARY ===');
    console.log('Nodes:', Object.keys(allNodes).length);
    console.log('Edges:', allEdges.length);
    
    // Validate volatile table ownership
    let volatileTableCount = 0;
    let volatileTableViolations = 0;
    Object.entries(allNodes).forEach(([nodeId, node]) => {
        if (node.is_volatile) {
            volatileTableCount++;
            if (node.owners.length > 1) {
                volatileTableViolations++;
            }
        }
    });
    
    console.log(`Volatile tables: ${volatileTableCount} total, ${volatileTableViolations} with multiple owners`);
    if (volatileTableViolations > 0) {
        console.error(`❌ ${volatileTableViolations} volatile table(s) have multiple owners - this should not happen!`);
    } else if (volatileTableCount > 0) {
        console.log(`✅ All ${volatileTableCount} volatile table(s) have single owners`);
    }
    
    // Log node details for debugging
    console.log('Node details:');
    Object.entries(allNodes).forEach(([nodeId, node]) => {
        console.log(`  ${nodeId}: ${node.name} (${node.is_volatile ? 'volatile' : 'global'}) - Owners: ${node.owners.join(', ')}`);
    });
    
    // Log edge details for debugging
    console.log('Edge details:');
    allEdges.forEach(([from, to, operations]) => {
        console.log(`  ${from} -> ${to} (${operations.length} operations)`);
    });
    
    console.log('=== OWNERSHIP MODEL BUILD COMPLETE ===');
}






// Calculate optimized levels for better horizontal layout
function calculateOptimizedLevels(nodes, edges) {
    const optimizedLevels = {};
    const nodeMap = new Map(nodes.map(node => [node.id, node]));
    
    // Calculate edge counts for all nodes
    const outgoingEdgeCounts = {};
    const incomingEdgeCounts = {};
    
    nodes.forEach(node => {
        outgoingEdgeCounts[node.id] = getOutgoingEdges(node.id, edges).length;
        incomingEdgeCounts[node.id] = getIncomingEdges(node.id, edges).length;
    });
    
    // Find source nodes (nodes with no incoming edges)
    const sourceNodes = nodes.filter(node => incomingEdgeCounts[node.id] === 0);
    
    // Initialize levels for source nodes
    sourceNodes.forEach(node => {
        optimizedLevels[node.id] = 0;
    });
    
    // Use topological sort with optimization for horizontal layout
    const visited = new Set();
    const queue = [...sourceNodes];
    
    while (queue.length > 0) {
        const currentNode = queue.shift();
        
        if (visited.has(currentNode.id)) {
            continue;
        }
        
        visited.add(currentNode.id);
        const currentLevel = optimizedLevels[currentNode.id] || 0;
        
        // Find all outgoing edges from this node
        const outgoingEdges = edges.filter(([from, to]) => from === currentNode.id);
        
        outgoingEdges.forEach(([from, to]) => {
            const targetNode = nodeMap.get(to);
            if (targetNode) {
                // Calculate target level based on data flow
                const targetLevel = Math.max(optimizedLevels[to] || 0, currentLevel + 1);
                optimizedLevels[to] = targetLevel;
                
                // Add to queue if not visited
                if (!visited.has(to)) {
                    queue.push(targetNode);
                }
            }
        });
    }
    
    // Handle any remaining nodes (cycles or isolated nodes)
    nodes.forEach(node => {
        if (!(node.id in optimizedLevels)) {
            optimizedLevels[node.id] = 0;
        }
    });
    
    // Separate red nodes (final targets) into their own levels
    // Red nodes should be positioned in the deepest/highest levels
    const redNodes = nodes.filter(node => getOutgoingEdges(node.id, edges).length === 0);
    const nonRedNodes = nodes.filter(node => getOutgoingEdges(node.id, edges).length > 0);
    
    // Find the maximum level for non-red nodes
    let maxNonRedLevel = 0;
    nonRedNodes.forEach(node => {
        maxNonRedLevel = Math.max(maxNonRedLevel, optimizedLevels[node.id] || 0);
    });
    
    // Position red nodes in levels after the maximum non-red level
    redNodes.forEach(node => {
        optimizedLevels[node.id] = maxNonRedLevel + 1;
    });
    
    // Group nodes by optimized level
    const levelGroups = {};
    nodes.forEach(node => {
        const level = optimizedLevels[node.id];
        if (!levelGroups[level]) {
            levelGroups[level] = [];
        }
        levelGroups[level].push(node);
    });
    
    // Create final optimized level map
    const sortedLevels = Object.keys(levelGroups).map(Number).sort((a, b) => a - b);
    const optimizedLevelMap = {};
    
    sortedLevels.forEach((level, index) => {
        const nodesInLevel = levelGroups[level];
        nodesInLevel.forEach(node => {
            optimizedLevelMap[node.id] = index;
        });
    });
    
    console.log('Optimized levels:', optimizedLevelMap);
    console.log(`Red nodes positioned in level ${maxNonRedLevel + 1} (after non-red nodes)`);
    console.log(`Red nodes: ${redNodes.map(n => n.name).join(', ')}`);
    return optimizedLevelMap;
}

// Calculate hierarchical levels with color-based separation for flow view
function calculateHierarchicalLevelsWithColorSeparation(nodes, edges) {
    const hierarchicalLevels = {};
    const nodeMap = new Map(nodes.map(node => [node.id, node]));
    
    // Calculate optimized levels based on actual data flow
    const optimizedLevels = calculateOptimizedLevels(nodes, edges);
    
    // Group nodes by optimized level
    const levelGroups = {};
    nodes.forEach(node => {
        const level = optimizedLevels[node.id];
        if (!levelGroups[level]) {
            levelGroups[level] = [];
        }
        levelGroups[level].push(node);
    });
    
    // Calculate positions for each level with color separation
    const levelSpacing = 200; // Reduced horizontal spacing between levels
    const nodeSpacing = 150; // Vertical spacing between nodes
    const columnWidth = 150; // Reduced width between columns for tighter layout
    
    Object.keys(levelGroups).forEach(levelStr => {
        const level = parseInt(levelStr);
        const nodesInLevel = levelGroups[level];
        
        // Separate nodes by color based on their role in the data flow
        const outgoingEdgeCounts = {};
        const incomingEdgeCounts = {};
        
        // Calculate edge counts for each node in this level
        nodesInLevel.forEach(node => {
            outgoingEdgeCounts[node.id] = getOutgoingEdges(node.id, edges).length;
            incomingEdgeCounts[node.id] = getIncomingEdges(node.id, edges).length;
        });
        
        // Separate nodes by their color classification
        // Red nodes (no targets) will be positioned rightmost
        const redNodes = nodesInLevel.filter(node => outgoingEdgeCounts[node.id] === 0); // No targets
        const greenNodes = nodesInLevel.filter(node => incomingEdgeCounts[node.id] === 0); // No sources
        const orangeNodes = nodesInLevel.filter(node => node.is_volatile); // Volatile tables
        const blueNodes = nodesInLevel.filter(node => 
            outgoingEdgeCounts[node.id] > 0 && 
            incomingEdgeCounts[node.id] > 0 && 
            !node.is_volatile
        ); // Intermediate tables
        
        // Calculate x position (level * spacing)
        const x = level * levelSpacing;
        
        // Position nodes by color type with optimized column placement
        // Column 0: Green nodes (no sources) - Source tables
        const greenTotalHeight = (greenNodes.length - 1) * nodeSpacing;
        const greenStartY = -greenTotalHeight / 2;
        
        greenNodes.forEach((node, index) => {
            const y = greenStartY + index * nodeSpacing;
            
            hierarchicalLevels[node.id] = {
                x: x,
                y: y,
                level: level,
                column: 0, // Left column for green nodes (sources)
                color: 'green'
            };
        });
        
        // Optimize column placement based on node counts
        let currentColumn = 1;
        let currentX = x + columnWidth;
        
        // Place orange nodes (volatile) - Volatile tables
        if (orangeNodes.length > 0) {
            const orangeTotalHeight = (orangeNodes.length - 1) * nodeSpacing;
            const orangeStartY = -orangeTotalHeight / 2;
            
            orangeNodes.forEach((node, index) => {
                const y = orangeStartY + index * nodeSpacing;
                
                hierarchicalLevels[node.id] = {
                    x: currentX,
                    y: y,
                    level: level,
                    column: currentColumn,
                    color: 'orange'
                };
            });
            currentColumn++;
            currentX += columnWidth;
        }
        
        // Place blue nodes (intermediate) - Intermediate tables
        if (blueNodes.length > 0) {
            const blueTotalHeight = (blueNodes.length - 1) * nodeSpacing;
            const blueStartY = -blueTotalHeight / 2;
            
            blueNodes.forEach((node, index) => {
                const y = blueStartY + index * nodeSpacing;
                
                hierarchicalLevels[node.id] = {
                    x: currentX,
                    y: y,
                    level: level,
                    column: currentColumn,
                    color: 'blue'
                };
            });
            currentColumn++;
            currentX += columnWidth;
        }
        
        // Place red nodes (no targets) - Final target tables (rightmost)
        // Red nodes should be in their own separate columns, not mixed with other colors
        if (redNodes.length > 0) {
            // Only split red nodes into multiple columns if there are more than 10 red nodes
            const redColumns = redNodes.length > 10 ? Math.ceil(redNodes.length / 5) : 1;
            
            if (redColumns > 1) {
                // Split red nodes into multiple columns but keep them rightmost
                const nodesPerRedColumn = Math.ceil(redNodes.length / redColumns);
                
                redNodes.forEach((node, index) => {
                    const column = Math.floor(index / nodesPerRedColumn);
                    const columnIndex = index % nodesPerRedColumn;
                    
                    const nodeX = currentX + column * (columnWidth / redColumns);
                    const totalHeight = (nodesPerRedColumn - 1) * nodeSpacing;
                    const startY = -totalHeight / 2;
                    const y = startY + columnIndex * nodeSpacing;
                    
                    hierarchicalLevels[node.id] = {
                        x: nodeX,
                        y: y,
                        level: level,
                        column: currentColumn + column,
                        color: 'red'
                    };
                });
            } else {
                // Single column for red nodes - position them all in the rightmost column
                const redTotalHeight = (redNodes.length - 1) * nodeSpacing;
                const redStartY = -redTotalHeight / 2;
                
                redNodes.forEach((node, index) => {
                    const y = redStartY + index * nodeSpacing;
                    
                    hierarchicalLevels[node.id] = {
                        x: currentX,
                        y: y,
                        level: level,
                        column: currentColumn,
                        color: 'red'
                    };
                });
            }
        }
        
        // Handle levels with many nodes (more than 10 total) for other colors
        if (nodesInLevel.length > 10) {
            // Calculate number of sub-columns needed for each color type
            const greenColumns = Math.ceil(greenNodes.length / 5);
            const orangeColumns = Math.ceil(orangeNodes.length / 5);
            const blueColumns = Math.ceil(blueNodes.length / 5);
            
            // Recalculate green node positions with multiple columns
            if (greenColumns > 1) {
                const nodesPerGreenColumn = Math.ceil(greenNodes.length / greenColumns);
                
                greenNodes.forEach((node, index) => {
                    const column = Math.floor(index / nodesPerGreenColumn);
                    const columnIndex = index % nodesPerGreenColumn;
                    
                    const nodeX = x + column * (columnWidth / greenColumns);
                    const totalHeight = (nodesPerGreenColumn - 1) * nodeSpacing;
                    const startY = -totalHeight / 2;
                    const y = startY + columnIndex * nodeSpacing;
                    
                    hierarchicalLevels[node.id] = {
                        x: nodeX,
                        y: y,
                        level: level,
                        column: column,
                        color: 'green'
                    };
                });
            }
            
            // Recalculate orange node positions with multiple columns
            if (orangeColumns > 1 && orangeNodes.length > 0) {
                const nodesPerOrangeColumn = Math.ceil(orangeNodes.length / orangeColumns);
                
                const orangeX = x + columnWidth; // Orange nodes are in the second column
                
                orangeNodes.forEach((node, index) => {
                    const column = Math.floor(index / nodesPerOrangeColumn);
                    const columnIndex = index % nodesPerOrangeColumn;
                    
                    const nodeX = orangeX + column * (columnWidth / orangeColumns);
                    const totalHeight = (nodesPerOrangeColumn - 1) * nodeSpacing;
                    const startY = -totalHeight / 2;
                    const y = startY + columnIndex * nodeSpacing;
                    
                    hierarchicalLevels[node.id] = {
                        x: nodeX,
                        y: y,
                        level: level,
                        column: column,
                        color: 'orange'
                    };
                });
            }
            
            // Recalculate blue node positions with multiple columns
            if (blueColumns > 1 && blueNodes.length > 0) {
                const nodesPerBlueColumn = Math.ceil(blueNodes.length / blueColumns);
                
                // Calculate blue node position - blue nodes come after orange nodes but before red nodes
                let blueX = x;
                if (orangeNodes.length > 0) blueX += columnWidth;
                blueX += columnWidth; // Blue nodes are after orange nodes
                
                blueNodes.forEach((node, index) => {
                    const column = Math.floor(index / nodesPerBlueColumn);
                    const columnIndex = index % nodesPerBlueColumn;
                    
                    const nodeX = blueX + column * (columnWidth / blueColumns);
                    const totalHeight = (nodesPerBlueColumn - 1) * nodeSpacing;
                    const startY = -totalHeight / 2;
                    const y = startY + columnIndex * nodeSpacing;
                    
                    hierarchicalLevels[node.id] = {
                        x: nodeX,
                        y: y,
                        level: level,
                        column: column,
                        color: 'blue'
                    };
                });
            }
        }
    });
    
    // Log level distribution for debugging
    console.log('Level distribution with color separation:');
    Object.keys(levelGroups).forEach(levelStr => {
        const level = parseInt(levelStr);
        const nodesInLevel = levelGroups[level];
        
        // Calculate counts for each color type
        const outgoingEdgeCounts = {};
        const incomingEdgeCounts = {};
        nodesInLevel.forEach(node => {
            outgoingEdgeCounts[node.id] = getOutgoingEdges(node.id, edges).length;
            incomingEdgeCounts[node.id] = getIncomingEdges(node.id, edges).length;
        });
        
        const redCount = nodesInLevel.filter(node => outgoingEdgeCounts[node.id] === 0).length;
        const greenCount = nodesInLevel.filter(node => incomingEdgeCounts[node.id] === 0).length;
        const orangeCount = nodesInLevel.filter(node => node.is_volatile).length;
        const blueCount = nodesInLevel.filter(node => 
            outgoingEdgeCounts[node.id] > 0 && 
            incomingEdgeCounts[node.id] > 0 && 
            !node.is_volatile
        ).length;
        
        console.log(`  Level ${level}: ${nodesInLevel.length} nodes (${redCount} red, ${greenCount} green, ${orangeCount} orange, ${blueCount} blue)`);
        
        // Debug red node positioning
        if (redCount > 0) {
            const redNodeIds = nodesInLevel.filter(node => {
                const outgoingEdgeCount = getOutgoingEdges(node.id, edges).length;
                return outgoingEdgeCount === 0;
            }).map(node => node.name);
            console.log(`    -> Red nodes (rightmost): ${redNodeIds.join(', ')}`);
            console.log(`    -> Red nodes will be positioned in separate column(s) to avoid mixing with other colors`);
        }
        
        if (redCount > 0) {
            const redColumns = redCount > 10 ? Math.ceil(redCount / 5) : 1;
            console.log(`    -> Red nodes: ${redColumns} column(s) (rightmost) - ${redCount} total red nodes`);
        }
        
        if (nodesInLevel.length > 10) {
            const greenColumns = Math.ceil(greenCount / 5);
            const orangeColumns = Math.ceil(orangeCount / 5);
            const blueColumns = Math.ceil(blueCount / 5);
            console.log(`    -> Green: ${greenColumns} columns, Orange: ${orangeColumns} columns, Blue: ${blueColumns} columns`);
        }
    });
    
    return hierarchicalLevels;
}

// Helper function to convert operation indices to script names with local indices
function getOperationDisplayText(operations) {
    // operations: Array of strings like "CAMSTAR_LOT_BONUS.sh::op4"
    if (!operations || operations.length === 0) return '';
    
    // Group operations by script
    const scriptGroups = {};
    operations.forEach(op => {
        const [scriptName, opId] = op.split('::');
        const opIndex = opId.replace('op', '');
        if (!scriptGroups[scriptName]) {
            scriptGroups[scriptName] = [];
        }
        scriptGroups[scriptName].push(opIndex);
    });
    
    // Convert to display format
    return Object.entries(scriptGroups)
        .map(([scriptName, indices]) => `${scriptName}:${indices.join('|')}`)
        .join(', ');
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
            // Reset network data before loading new data
            resetNetworkData();
            
            lineageData = JSON.parse(e.target.result);
            // Add source file information if not present
            if (!lineageData.source_file) {
                lineageData.source_file = file.name;
            }
            
            // Create script structure for single file to maintain consistency
            const fileName = file.name.replace('_lineage.json', '');
            lineageData.scripts = {
                [fileName]: {
                    script_name: fileName,
                    tables: lineageData.tables || {},
                    bteq_statements: lineageData.bteq_statements || []
                }
            };
            initializeScriptNames();
            
            displaySummary();
            displayTables();
            displayStatements();
            displayNetworkFileGroups();
            document.getElementById('summarySection').style.display = 'block';
            document.getElementById('tabSection').style.display = 'block';
            initializeTableNames();
            // initializeScriptSearchInputEvents();
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

    // Initialize merged data structure with script-based organization
    const mergedData = {
        scripts: {},
        source_file: `${jsonFiles.length}`
    };

    let processedFiles = 0;
    const totalFiles = jsonFiles.length;

    jsonFiles.forEach((file, index) => {
        const reader = new FileReader();
        reader.onload = function(e) {
            try {
                const data = JSON.parse(e.target.result);
                console.log(`Processing file: ${file.name}`);
                
                // Create script entry using script_name from the data
                const scriptName = data.script_name || file.name.replace('_lineage.json', '');
                mergedData.scripts[scriptName] = {
                    script_name: scriptName,
                    tables: data.tables || {},
                    bteq_statements: data.bteq_statements || []
                };
                
                processedFiles++;
                
                // When all files are processed, update the display
                if (processedFiles === totalFiles) {
                    console.log(`Successfully merged ${totalFiles} files from uploaded folder`);
                    
                    // Reset network data before loading new data
                    resetNetworkData();
                    
                    lineageData = mergedData;
                    initializeScriptNames();
                    buildOwnershipModel();
                    displaySummary();
                    displayTables();
                    displayStatements();
                    displayNetworkFileGroups();
                    document.getElementById('summarySection').style.display = 'block';
                    document.getElementById('tabSection').style.display = 'block';
                    initializeTableNames();
                    // initializeScriptSearchInputEvents();
                    
                    // Update URL to include the folder path
                    const url = new URL(window.location);
                    url.searchParams.set('folder', 'uploaded');
                    window.history.pushState({}, '', url);
                }
            } catch (error) {
                console.error(`Error parsing JSON file ${file.name}:`, error);
                showError(`Error parsing JSON file ${file.name}: ${error.message}`);
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
            // Reset network data before loading new data
            resetNetworkData();
            
            lineageData = data;
            // Add source file information if not present
            if (!lineageData.source_file) {
                lineageData.source_file = jsonPath;
            }
            // Create script structure for single file to maintain consistency
            const fileName = jsonPath.split('/').pop().replace('_lineage.json', '');
            lineageData.scripts = {
                [fileName]: {
                    script_name: fileName,
                    tables: lineageData.tables || {},
                    bteq_statements: lineageData.bteq_statements || []
                }
            };
            initializeScriptNames();
            
            displaySummary();
            displayTables();
            displayStatements();
            displayNetworkFileGroups();
            document.getElementById('summarySection').style.display = 'block';
            document.getElementById('tabSection').style.display = 'block';
            initializeTableNames();
            // initializeScriptSearchInputEvents();
            
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
    
    // Handle both legacy single-file structure and new script-based structure
    let totalTables = 0;
    let sourceTables = 0;
    let targetTables = 0;
    let volatileTables = 0;
    let totalOperations = 0;
    
    if (lineageData.scripts) {
        // New script-based structure
        Object.values(lineageData.scripts).forEach(scriptData => {
            const tables = scriptData.tables || {};
            totalTables += Object.keys(tables).length;
            sourceTables += Object.values(tables).filter(t => t.source && t.source.length > 0).length;
            targetTables += Object.values(tables).filter(t => t.target && t.target.length > 0).length;
            volatileTables += Object.values(tables).filter(t => t.is_volatile).length;
        });
        
        // Sum up all operations from all scripts
        Object.values(lineageData.scripts).forEach(scriptData => {
            totalOperations += (scriptData.bteq_statements || []).length;
        });
    } else {
        // Legacy single file structure
        const tables = lineageData.tables || {};
        totalTables = Object.keys(tables).length;
        sourceTables = Object.values(tables).filter(t => t.source && t.source.length > 0).length;
        targetTables = Object.values(tables).filter(t => t.target && t.target.length > 0).length;
        volatileTables = Object.values(tables).filter(t => t.is_volatile).length;
        totalOperations = (lineageData.bteq_statements || []).length;
    }

    // Determine script count and display name(s)
    let scriptCount = 1;
    let scriptDisplay = '';
    if (lineageData.scripts && Object.keys(lineageData.scripts).length > 1) {
        scriptCount = Object.keys(lineageData.scripts).length;
        scriptDisplay = '';
    } else if (lineageData.scripts && Object.keys(lineageData.scripts).length === 1) {
        // Single file mode, show cleaned-up script name
        let src = Object.keys(lineageData.scripts)[0];
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
    
    // Check if we have scripts (folder mode) or single file
    if (lineageData.scripts) {
        // Tree view for multiple files
        tableList.innerHTML = `
            <div class="tree-view">
                ${Object.keys(lineageData.scripts).map(scriptName => {
                    const scriptData = lineageData.scripts[scriptName];
                    const tables = Object.keys(scriptData.tables || {}).sort();
                    

                    return `
                        <div class="file-group">
                            <div class="tree-toggle" onclick="toggleFileGroup('${scriptName}-tables')">
                                <span class="toggle-icon">▶</span>
                                <span>📄 ${scriptName} (${tables.length})</span>
                            </div>
                            <div class="tree-children" id="${scriptName}-tables">
                                ${tables.map(tableName => {
                                    const table = scriptData.tables[tableName];
                                    const isVolatile = table.is_volatile;
                                    const className = `tree-item ${isVolatile ? 'volatile' : ''}`;
                                    
                                    return `
                                        <div class="${className}" onclick="selectTable('${tableName}')">
                                            <div style="font-weight: bold;">${tableName}</div>
                                            <div style="font-size: 0.8em; color: #6c757d;">
                                                ${(table.source || []).length} sources, ${(table.target || []).length} targets
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
        const tables = Object.keys(lineageData.tables || {}).sort();
        
        tableList.innerHTML = tables.map(tableName => {
            const table = lineageData.tables[tableName];
            const isVolatile = table.is_volatile;
            const className = `table-item ${isVolatile ? 'volatile' : ''}`;
            
            return `
                <li class="${className}" onclick="selectTable('${tableName}')">
                    <div style="font-weight: bold;">${tableName}</div>
                    <div style="font-size: 0.8em; color: #6c757d;">
                        ${(table.source || []).length} sources, ${(table.target || []).length} targets
                        ${isVolatile ? ' (volatile)' : ''}
                    </div>
                </li>
            `;
        }).join('');
    }
}

function displayStatements() {
    const statementList = document.getElementById('statementList');
    
    // Check if we have scripts (folder mode) or single file
    if (lineageData.scripts) {
        // Tree view for multiple files
        statementList.innerHTML = `
            <div class="tree-view">
                ${Object.keys(lineageData.scripts).map(scriptName => {
                    const scriptData = lineageData.scripts[scriptName];
                    const statements = scriptData.bteq_statements || [];
                    if (statements.length === 0) return '';

                    // Use the script name and clean it up
                    let displayScriptName = scriptName;
                    displayScriptName = displayScriptName.split('/').pop();
                    displayScriptName = displayScriptName.replace(/\.json$/i, '');
                    if (displayScriptName.match(/_sh_lineage$/i)) {
                        displayScriptName = displayScriptName.replace(/_sh_lineage$/i, '.sh');
                    } else if (displayScriptName.match(/_ksh_lineage$/i)) {
                        displayScriptName = displayScriptName.replace(/_ksh_lineage$/i, '.ksh');
                    } else if (displayScriptName.match(/_sql_lineage$/i)) {
                        displayScriptName = displayScriptName.replace(/_sql_lineage$/i, '.sql');
                    } else if (displayScriptName.match(/_lineage$/i)) {
                        displayScriptName = displayScriptName.replace(/_lineage$/i, '');
                    }

                    return `
                        <div class="file-group">
                            <div class="tree-toggle" onclick="toggleFileGroup('${scriptName}-statements')">
                                <span class="toggle-icon">▶</span>
                                <span>📄 ${displayScriptName} (${statements.length})</span>
                            </div>
                            <div class="tree-children" id="${scriptName}-statements">
                                ${statements.map((statement, localIndex) => {
                                    const firstLine = statement.split('\n')[0].trim();
                                    const displayText = firstLine.length > 60 ? firstLine.substring(0, 60) + '...' : firstLine;
                                    return `
                                        <div class="tree-item" onclick="selectStatementByScript('${scriptName}', ${localIndex})">
                                            <div style="font-weight: bold;">Statement ${displayScriptName}:${localIndex}</div>
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
        const statements = lineageData.bteq_statements || [];
        let scriptName = 'Unknown';
        if (lineageData.source_file) {
            scriptName = lineageData.source_file.split('/').pop();
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
        }
        statementList.innerHTML = statements.map((statement, index) => {
            const firstLine = statement.split('\n')[0].trim();
            const displayText = firstLine.length > 60 ? firstLine.substring(0, 60) + '...' : firstLine;
            return `
                <li class="statement-item" onclick="selectStatement(${index})">
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
    const scriptData = lineageData.scripts[scriptKey];
    if (!scriptData) {
        contentArea.innerHTML = `<div class="error">Script not found.</div>`;
        return;
    }
    const statement = (scriptData.bteq_statements || [])[localIndex];
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
    
    if (!lineageData.scripts) {
        networkFileGroups.innerHTML = '<p style="color: #6c757d; font-size: 0.9em;">Single file mode - use "All Tables Network"</p>';
        return;
    }
    
    networkFileGroups.innerHTML = `
        <div class="tree-view">
            ${Object.keys(lineageData.scripts).map(scriptName => {
                const scriptData = lineageData.scripts[scriptName];
                const tables = Object.keys(scriptData.tables || {});
                if (tables.length === 0) return '';

                // Use the script name and clean it up
                let displayScriptName = scriptName;
                // Remove path if present
                displayScriptName = displayScriptName.split('/').pop();
                // Remove .json extension if present
                displayScriptName = displayScriptName.replace(/\.json$/i, '');
                // Convert *_sh_lineage or *_ksh_lineage to .sh/.ksh
                if (displayScriptName.match(/_sh_lineage$/i)) {
                    displayScriptName = displayScriptName.replace(/_sh_lineage$/i, '.sh');
                } else if (displayScriptName.match(/_ksh_lineage$/i)) {
                    displayScriptName = displayScriptName.replace(/_ksh_lineage$/i, '.ksh');
                } else if (displayScriptName.match(/_sql_lineage$/i)) {
                    displayScriptName = displayScriptName.replace(/_sql_lineage$/i, '.sql');
                } else if (displayScriptName.match(/_lineage$/i)) {
                    displayScriptName = displayScriptName.replace(/_lineage$/i, '');
                }

                return `
                    <div class="file-group">
                        <div class="tree-toggle" onclick="toggleFileGroup('${scriptName}-network')">
                            <span class="toggle-icon">▶</span>
                            <span style="cursor:pointer;" onclick="event.stopPropagation(); showFileNetwork('${scriptName}')">📄 ${displayScriptName} (${tables.length})</span>
                        </div>
                        <div class="tree-children" id="${scriptName}-network">
                            ${tables.map(tableName => {
                                const table = scriptData.tables[tableName];
                                const isVolatile = table.is_volatile;
                                const className = `tree-item ${isVolatile ? 'volatile' : ''}`;
                                
                                return `
                                    <div class="${className}" onclick="showTableNetwork('${tableName}')">
                                        <div style="font-weight: bold;">${tableName}</div>
                                        <div style="font-size: 0.8em; color: #6c757d;">
                                            ${(table.source || []).length} sources, ${(table.target || []).length} targets
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
    
    // Handle legacy single file mode
    if (!lineageData.scripts) {
        const statement = (lineageData.bteq_statements || [])[statementIndex];
        const scriptName = lineageData.source_file ? lineageData.source_file.replace('_lineage.json', '') : 'Unknown';
        
        contentArea.innerHTML = `
            <div class="table-details">
                <div class="table-header">
                    <div class="table-name">SQL Statement ${scriptName}:${statementIndex}</div>
                </div>
                
                <div style="background: #f8f9fa; padding: 20px; border-radius: 8px; margin-top: 20px;">
                    <h4 style="margin-bottom: 15px; color: #495057;">Formatted SQL:</h4>
                    <div style="background: white; padding: 20px; border-radius: 8px; font-family: 'Courier New', monospace; white-space: pre-wrap; font-size: 14px; line-height: 1.5; max-height: 500px; overflow-y: auto; border: 1px solid #dee2e6;">
${statement}
                    </div>
                </div>
            </div>
        `;
    } else {
        // For script-based mode, redirect to the appropriate script function
        // Find which script contains this statement
        let foundScript = null;
        let localIndex = statementIndex;
        
        for (const [scriptName, scriptData] of Object.entries(lineageData.scripts)) {
            if (scriptData.bteq_statements && localIndex < scriptData.bteq_statements.length) {
                foundScript = scriptName;
                break;
            }
            localIndex -= scriptData.bteq_statements ? scriptData.bteq_statements.length : 0;
        }
        
        if (foundScript) {
            displayStatementDetailsByScript(foundScript, localIndex);
        } else {
            contentArea.innerHTML = `
                <div class="error">
                    <strong>Error:</strong> Statement not found
                </div>
            `;
        }
    }
}

function displayTableDetails(tableName) {
    const contentArea = document.getElementById('contentArea');
    
    // Find the table in any script
    let table = null;
    let scriptName = null;
    if (lineageData.scripts) {
        for (const [sName, sData] of Object.entries(lineageData.scripts)) {
            if (sData.tables[tableName]) {
                table = sData.tables[tableName];
                scriptName = sName;
                break;
            }
        }
    } else {
        // Fallback for legacy single file mode
        table = lineageData.tables[tableName];
    }
    
    // Use script name if available, otherwise use a fallback
    const operationScriptName = scriptName || 'Unknown';
    
    const sourceRelationships = (table.source || []).map(rel => `
        <div class="relationship-item">
            <div class="table-name">${rel.name}</div>
            <div class="operations">
                Operations: 
                <div class="operation-list">
                    ${(rel.operation || []).map(opIndex => `
                        <span class="operation-badge" onclick="showSql('${operationScriptName}::op${opIndex}')">
                            ${operationScriptName}:${opIndex}
                        </span>
                    `).join('')}
                </div>
            </div>
        </div>
    `).join('');

    const targetRelationships = (table.target || []).map(rel => `
        <div class="relationship-item">
            <div class="table-name">${rel.name}</div>
            <div class="operations">
                Operations: 
                <div class="operation-list">
                    ${(rel.operation || []).map(opIndex => `
                        <span class="operation-badge" onclick="showSql('${operationScriptName}::op${opIndex}')">
                            ${operationScriptName}:${opIndex}
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
                    <h4>📥 Source Tables (${(table.source || []).length})</h4>
                    ${(table.source || []).length > 0 ? sourceRelationships : '<p style="color: #6c757d;">No source tables</p>'}
                </div>
                
                <div class="relationship-section">
                    <h4>📤 Target Tables (${(table.target || []).length})</h4>
                    ${(table.target || []).length > 0 ? targetRelationships : '<p style="color: #6c757d;">No target tables</p>'}
                </div>
            </div>
        </div>
    `;
}

// Helper function to parse operation in new format (e.g., "CAMSTAR_LOT_BONUS.sh::op4")
function parseOperation(operationString) {
    if (typeof operationString !== 'string' || !operationString.includes('::')) {
        console.error('Invalid operation format:', operationString);
        throw new Error(`Invalid operation format: ${operationString}. Expected format: "scriptName::opIndex"`);
    }
    
    const [scriptName, opId] = operationString.split('::');
    const opIndex = parseInt(opId.replace('op', ''), 10);
    
    if (isNaN(opIndex)) {
        console.error('Invalid operation index:', opId);
        throw new Error(`Invalid operation index: ${opId}`);
    }
    
    return {
        scriptName: scriptName,
        localIndex: opIndex,
        displayText: `${scriptName}:${opIndex}`,
        operationKey: operationString
    };
}

function showSql(operationString) {
    const modal = document.getElementById('sqlModal');
    const modalTitle = document.getElementById('modalTitle');
    const sqlContent = document.getElementById('sqlContent');
    
    // Parse the operation string to get script and index
    const operationInfo = parseOperation(operationString);
    
    modalTitle.textContent = `SQL Statement ${operationInfo.displayText}`;
    
    // Get the SQL statement from the appropriate script
    let sqlStatement = null;
    if (lineageData.scripts && lineageData.scripts[operationInfo.scriptName]) {
        const scriptData = lineageData.scripts[operationInfo.scriptName];
        if (scriptData.bteq_statements && scriptData.bteq_statements.length > operationInfo.localIndex) {
            sqlStatement = scriptData.bteq_statements[operationInfo.localIndex];
        }
    } else if (!lineageData.scripts && lineageData.bteq_statements) {
        // Legacy single file mode
        sqlStatement = lineageData.bteq_statements[operationInfo.localIndex];
    }
    
    sqlContent.textContent = sqlStatement || 'Statement not found';
    modal.style.display = 'block';
}

function closeSqlModal() {
    document.getElementById('sqlModal').style.display = 'none';
}

function showScriptOperations(scriptName) {
    const modal = document.getElementById('sqlModal');
    const modalTitle = document.getElementById('modalTitle');
    const sqlContent = document.getElementById('sqlContent');
    
    // Get the script data
    let scriptData = null;
    if (lineageData.scripts && lineageData.scripts[scriptName]) {
        scriptData = lineageData.scripts[scriptName];
    }
    
    if (!scriptData || !scriptData.bteq_statements) {
        modalTitle.textContent = `Script Operations: ${scriptName}`;
        sqlContent.textContent = 'No operations found for this script.';
        modal.style.display = 'block';
        return;
    }
    
    modalTitle.textContent = `Script Operations: ${scriptName}`;
    
    // Get current filtered data to find operations for this script
    const { edges: filteredEdges } = applyFilters(
        selectedNetworkScript ? [selectedNetworkScript] : [], 
        selectedTableFilters, 
        connectionMode
    );
    
    // Collect all operations for this script from the current network view
    const scriptOperations = new Set();
    filteredEdges.forEach(([from, to, operations]) => {
        if (operations && operations.length > 0) {
            operations.forEach(op => {
                const [opScriptName, opId] = op.split('::');
                if (opScriptName === scriptName) {
                    scriptOperations.add(opId.replace('op', ''));
                }
            });
        }
    });
    
    // Sort operations by index
    const sortedOperations = Array.from(scriptOperations).map(opIndex => parseInt(opIndex, 10)).sort((a, b) => a - b);
    
    if (sortedOperations.length === 0) {
        sqlContent.textContent = 'No operations found for this script in the current network view.';
        modal.style.display = 'block';
        return;
    }
    
    // Create content showing all operations for this script
    let content = `<div style="margin-bottom: 20px;">
        <h4 style="color: #495057; margin-bottom: 10px;">Operations in Current Network View</h4>
        <p><strong>Script:</strong> ${scriptName}</p>
        <p><strong>Operations:</strong> ${sortedOperations.length} found</p>
    </div>
    <div style="border-top: 1px solid #dee2e6; padding-top: 20px;">
        <h4 style="color: #495057; margin-bottom: 15px;">SQL Statements</h4>`;
    
    // Add each SQL statement
    sortedOperations.forEach(opIndex => {
        const sqlStatement = scriptData.bteq_statements[opIndex];
        if (sqlStatement) {
            const operationString = `${scriptName}::op${opIndex}`;
            content += `
                <div style="background: white; padding: 15px; border-radius: 8px; margin-bottom: 15px; border: 1px solid #dee2e6;">
                    <h5 style="color: #007bff; margin-bottom: 10px; cursor: pointer;" onclick="showSql('${operationString}')">${scriptName}:${opIndex}</h5>
                    <div style="background: #f8f9fa; padding: 12px; border-radius: 6px; font-family: 'Courier New', monospace; white-space: pre-wrap; font-size: 12px; line-height: 1.4; max-height: 200px; overflow-y: auto; border: 1px solid #e9ecef;">
${sqlStatement}
                    </div>
                </div>
            `;
        }
    });
    
    content += `</div>`;
    sqlContent.innerHTML = content;
    modal.style.display = 'block';
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
                // Ensure ownership model is built
                if (Object.keys(allNodes).length === 0) {
                    buildOwnershipModel();
                }
                createNetworkVisualization([], []);
            }, 100);
        }
    }
}

// Helper function to filter out self-referencing edges
function filterNonSelfReferencingEdges(edges) {
    return edges.filter(([from, to]) => from !== to);
}

// Helper function to get outgoing edges for a node (excluding self-references)
function getOutgoingEdges(nodeId, edges) {
    return edges.filter(([from, to]) => from === nodeId && from !== to);
}

// Helper function to get incoming edges for a node (excluding self-references)
function getIncomingEdges(nodeId, edges) {
    return edges.filter(([from, to]) => to === nodeId && from !== to);
}

// Helper function to determine node color based on edge relationships and volatility
function getNodeColor(node, filteredEdges = []) {
    // Calculate source and target counts based on the current filtered edges
    const outgoingEdgeCount = getOutgoingEdges(node.id, filteredEdges).length;
    const incomingEdgeCount = getIncomingEdges(node.id, filteredEdges).length;
    
    if (node.is_volatile) {
        return '#ff9800'; // Orange for volatile tables
    } else if (incomingEdgeCount === 0) {
        return '#28a745'; // Green for tables with no sources (no incoming edges)
    } else if (outgoingEdgeCount === 0) {
        return '#dc3545'; // Red for tables with no targets (no outgoing edges)
    } else {
        return '#007bff'; // Blue for all other tables
    }
}

function createNetworkVisualization(scriptFilters = [], tableFilters = [], force = false) {
    const container = document.getElementById('networkContainer');
    
    // Check if filters have actually changed
    const currentFilters = { 
        scriptFilters: scriptFilters, 
        tableFilters: tableFilters, 
        mode: connectionMode 
    };
    
    // Compare with last applied filters
    const filtersChanged = JSON.stringify(currentFilters) !== JSON.stringify(lastNetworkFilters);
    
    if (!filtersChanged && !force && network) {
        console.log('Network filters unchanged, skipping recreation');
        return;
    }
    
    // Update last applied filters
    lastNetworkFilters = currentFilters;
    
    // Apply filters to get filtered data
    const { nodes: filteredNodes, edges: filteredEdges } = applyFilters(scriptFilters, tableFilters, connectionMode);
    
    // Create vis.js nodes
    const visNodes = filteredNodes.map(node => {
        const nodeColor = getNodeColor(node, filteredEdges);
        // Count outgoing and incoming edges for this node in the current filteredEdges
        const outgoingEdgeCount = getOutgoingEdges(node.id, filteredEdges).length;
        const incomingEdgeCount = getIncomingEdges(node.id, filteredEdges).length;
        const nodeSize = 20 + Math.min(incomingEdgeCount + outgoingEdgeCount, 10) * 2;
        
        return {
            id: node.id,
            label: node.name,
            title: `${node.name}\nSources: ${incomingEdgeCount}\nTargets: ${outgoingEdgeCount}\nVolatile: ${node.is_volatile ? 'Yes' : 'No'}\nOwners: ${node.owners.join(', ')}`,
            color: nodeColor,
            size: nodeSize,
            font: {
                size: 12,
                face: 'Arial'
            }
        };
    });
    
    // Create vis.js edges
    const visEdges = filteredEdges.map(([from, to, operations]) => {
        // Create operation display text using the new format
        const operationTexts = getOperationDisplayText(operations);
        
        return {
            from: from,
            to: to,
            arrows: 'to',
            color: { color: '#28a745', opacity: 0.8 },
            width: 2,
            title: `Operations: ${operationTexts}`,
            label: operationTexts,
            font: {
                size: 10,
                color: '#28a745'
            }
        };
    });
    
    // Debug: Print nodes used in the network
    console.log('=== Network Re-render Debug ===');
    console.log('Function: createNetworkVisualization');
    console.log('Script filters:', scriptFilters);
    console.log('Table filters:', tableFilters);
    console.log('Connection mode:', connectionMode);
    console.log('Total nodes:', visNodes.length);
    console.log('Nodes:', visNodes.map(n => n.id).sort());
    console.log('Total edges:', visEdges.length);
    
    // Additional debug info about volatile tables
    const volatileNodes = filteredNodes.filter(node => node.is_volatile);
    const globalNodes = filteredNodes.filter(node => !node.is_volatile);
    console.log(`Volatile tables: ${volatileNodes.length}, Global tables: ${globalNodes.length}`);
    
    if (volatileNodes.length > 0) {
        console.log('Volatile tables in network:', volatileNodes.map(n => `${n.name} (${n.owners.join(', ')})`));
    }
    
    console.log('================================');
    
    // Create the network
    const data = {
        nodes: new vis.DataSet(visNodes),
        edges: new vis.DataSet(visEdges)
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
            dragView: true,
            dragNodes: true,
            selectable: true,
            selectConnectedEdges: true
        }
    };
    
    // Add hierarchical layout when flow view is enabled
    if (flowViewEnabled) {
        // Calculate hierarchical levels with color-based separation
        const hierarchicalLevels = calculateHierarchicalLevelsWithColorSeparation(filteredNodes, filteredEdges);
        
        // Apply hierarchical positioning to nodes with color separation
        visNodes.forEach(node => {
            const level = hierarchicalLevels[node.id];
            if (level !== undefined) {
                node.x = level.x;
                node.y = level.y;
                // Allow dragging in Flow View by not fixing node positions
            }
        });
        
        // Update options for hierarchical layout
        options.layout = {
            hierarchical: {
                enabled: false // Disable vis.js hierarchical layout since we're doing manual positioning
            }
        };
        
        // Disable physics for hierarchical layout
        options.physics.enabled = false;
        
        console.log('Flow view enabled - using manual hierarchical layout with color separation');
        console.log('Hierarchical levels with color separation:', hierarchicalLevels);
    }
    
    if (network) {
        network.destroy();
    }
    
    network = new vis.Network(container, data, options);
    
    // Add zoom buttons after network is created
    addZoomButtons();
    
    // Physics is disabled to prevent animation
    network.on('stabilizationProgress', function(params) {
        // Physics is disabled
    });
    
    network.on('stabilizationIterationsDone', function(params) {
        // Physics remains disabled
        network.setOptions({
            physics: {
                enabled: false
            }
        });
    });
    
    // Add click events for nodes and edges
    network.on('click', function(params) {
        if (params.nodes.length > 0) {
            // Node clicked - apply filter to show this table and its relationships
            const clickedNodeId = params.nodes[0];
            
            // Only apply filter if lock view is disabled
            if (!lockViewEnabled) {
                showDirectlyRelatedNodes(clickedNodeId);
                hideSidePanel();
            } else {
                console.log('View is locked - node click ignored');
            }
        } else if (params.edges.length > 0) {
            // Edge clicked
            showEdgeDetails(params.edges[0]);
        } else {
            hideSidePanel();
        }
    });
    
    // Validate the network for volatile table violations
    validateNetworkVolatileTableRules(filteredNodes, filteredEdges);
    
    // Detect data modeling issues
    detectDataModelingIssues(filteredNodes, filteredEdges);
    
    // Print a quick summary
    getNetworkValidationSummary(filteredNodes, filteredEdges);
    
    // Perform comprehensive algorithm analysis
    analyzeNetworkAlgorithm(filteredNodes, filteredEdges);
}

// Validate volatile table rules in the network
function validateNetworkVolatileTableRules(nodes, edges) {
    console.log('=== VOLATILE TABLE VALIDATION ===');
    
    // Get all volatile nodes with their owners
    const volatileNodes = nodes.filter(node => node.is_volatile);
    const volatileNodeMap = new Map();
    
    volatileNodes.forEach(node => {
        volatileNodeMap.set(node.id, {
            name: node.name,
            owners: node.owners,
            isVolatile: node.is_volatile
        });
    });
    
    console.log(`Found ${volatileNodes.length} volatile tables in the network`);
    
    // Check each edge for volatile table violations and circular references
    const violations = [];
    const circularReferences = [];
    
    edges.forEach(([from, to, operations]) => {
        const fromNode = volatileNodeMap.get(from);
        const toNode = volatileNodeMap.get(to);
        
        // Check if both nodes are volatile
        if (fromNode && toNode) {
            // Both are volatile - check if they're owned by different scripts
            const fromOwners = new Set(fromNode.owners);
            const toOwners = new Set(toNode.owners);
            
            // Check if there's any overlap in ownership
            const hasCommonOwner = Array.from(fromOwners).some(owner => toOwners.has(owner));
            
            if (!hasCommonOwner) {
                violations.push({
                    from: fromNode.name,
                    fromOwners: fromNode.owners,
                    to: toNode.name,
                    toOwners: toNode.owners,
                    operations: operations,
                    type: 'cross_script_volatile_edge'
                });
            }
        }
        
        // Check for circular references (self-loops)
        if (from === to) {
            circularReferences.push({
                node: from,
                operations: operations
            });
        }
    });
    
    // Report volatile table violations
    if (violations.length === 0) {
        console.log('✅ No volatile table violations found');
        console.log('   All volatile table edges are within the same script scope');
    } else {
        console.error(`❌ Found ${violations.length} volatile table violations:`);
        violations.forEach((violation, index) => {
            console.error(`   ${index + 1}. ${violation.from} (${violation.fromOwners.join(', ')}) → ${violation.to} (${violation.toOwners.join(', ')})`);
            console.error(`      Operations: ${getOperationDisplayText(violation.operations)}`);
            console.error(`      Issue: Volatile table owned by different script`);
        });
    }
    
    // Report circular references
    if (circularReferences.length === 0) {
        console.log('✅ No circular references found');
    } else {
        console.warn(`⚠️ Found ${circularReferences.length} circular references (self-loops):`);
        circularReferences.forEach((ref, index) => {
            console.warn(`   ${index + 1}. ${ref.node} → ${ref.node}`);
            console.warn(`      Operations: ${getOperationDisplayText(ref.operations)}`);
        });
    }
    
    // Additional validation: Check for volatile tables with no owners
    const volatileWithoutOwners = volatileNodes.filter(node => !node.owners || node.owners.length === 0);
    if (volatileWithoutOwners.length > 0) {
        console.warn(`⚠️ Found ${volatileWithoutOwners.length} volatile tables without owners:`);
        volatileWithoutOwners.forEach(node => {
            console.warn(`   - ${node.name} (${node.id})`);
        });
    }
    
    // Additional validation: Check for volatile tables with multiple owners
    const volatileWithMultipleOwners = volatileNodes.filter(node => node.owners && node.owners.length > 1);
    if (volatileWithMultipleOwners.length > 0) {
        console.warn(`⚠️ Found ${volatileWithMultipleOwners.length} volatile tables with multiple owners:`);
        volatileWithMultipleOwners.forEach(node => {
            console.warn(`   - ${node.name} (${node.id}): [${node.owners.join(', ')}]`);
        });
    }
    
    console.log('=== END VOLATILE TABLE VALIDATION ===');
    
    return {
        totalVolatileTables: volatileNodes.length,
        violations: violations,
        circularReferences: circularReferences,
        volatileWithoutOwners: volatileWithoutOwners.length,
        volatileWithMultipleOwners: volatileWithMultipleOwners.length
    };
}

// Detect potential data modeling issues
function detectDataModelingIssues(nodes, edges) {
    console.log('=== DATA MODELING ISSUE DETECTION ===');
    
    // Find tables with the same name across different scripts
    const tableNameGroups = new Map();
    
    nodes.forEach(node => {
        const tableName = node.name;
        if (!tableNameGroups.has(tableName)) {
            tableNameGroups.set(tableName, []);
        }
        tableNameGroups.get(tableName).push(node);
    });
    
    const duplicateTableIssues = [];
    
    tableNameGroups.forEach((nodesWithSameName, tableName) => {
        if (nodesWithSameName.length > 1) {
            const scripts = nodesWithSameName.map(node => {
                const scriptName = node.id.includes('::') ? node.id.split('::')[0] : 'GLOBAL';
                return { script: scriptName, isVolatile: node.is_volatile, nodeId: node.id };
            });
            
            duplicateTableIssues.push({
                tableName: tableName,
                nodes: nodesWithSameName,
                scripts: scripts
            });
        }
    });
    
    if (duplicateTableIssues.length > 0) {
        console.warn(`⚠️ Found ${duplicateTableIssues.length} tables with duplicate names across scripts:`);
        duplicateTableIssues.forEach((issue, index) => {
            console.warn(`   ${index + 1}. Table: ${issue.tableName}`);
            issue.scripts.forEach(script => {
                console.warn(`      - ${script.script}: ${script.isVolatile ? 'VOLATILE' : 'GLOBAL'} (${script.nodeId})`);
            });
            console.warn(`      Recommendation: Consider making this a global table or renaming to be script-specific`);
        });
    } else {
        console.log('✅ No duplicate table names found across scripts');
    }
    
    console.log('=== END DATA MODELING ISSUE DETECTION ===');
    
    return duplicateTableIssues;
}

// Helper function to get a quick summary of network validation
function getNetworkValidationSummary(nodes, edges) {
    const validation = validateNetworkVolatileTableRules(nodes, edges);
    
    console.log('=== NETWORK VALIDATION SUMMARY ===');
    console.log(`Total nodes: ${nodes.length}`);
    console.log(`Total edges: ${edges.length}`);
    console.log(`Volatile tables: ${validation.totalVolatileTables}`);
    console.log(`Violations: ${validation.violations.length}`);
    console.log(`Circular references: ${validation.circularReferences.length}`);
    console.log(`Volatile tables without owners: ${validation.volatileWithoutOwners}`);
    console.log(`Volatile tables with multiple owners: ${validation.volatileWithMultipleOwners}`);
    
    if (validation.violations.length > 0) {
        console.log('❌ Network has volatile table violations!');
    } else if (validation.circularReferences.length > 0) {
        console.log('⚠️ Network has circular references!');
    } else {
        console.log('✅ Network passes validation');
    }
    console.log('=====================================');
    
    return validation;
}

// Comprehensive algorithm analysis function
function analyzeNetworkAlgorithm(nodes, edges) {
    console.log('=== ALGORITHM ANALYSIS ===');
    
    // Analyze node distribution
    const volatileNodes = nodes.filter(node => node.is_volatile);
    const globalNodes = nodes.filter(node => !node.is_volatile);
    const scriptOwners = new Map();
    
    nodes.forEach(node => {
        node.owners.forEach(owner => {
            if (!scriptOwners.has(owner)) {
                scriptOwners.set(owner, []);
            }
            scriptOwners.get(owner).push(node.name);
        });
    });
    
    console.log('Node Analysis:');
    console.log(`  Total nodes: ${nodes.length}`);
    console.log(`  Volatile nodes: ${volatileNodes.length}`);
    console.log(`  Global nodes: ${globalNodes.length}`);
    console.log(`  Scripts involved: ${scriptOwners.size}`);
    
    // Analyze edge distribution
    const edgeSources = new Map();
    const edgeTargets = new Map();
    
    edges.forEach(([from, to, operations]) => {
        edgeSources.set(from, (edgeSources.get(from) || 0) + 1);
        edgeTargets.set(to, (edgeTargets.get(to) || 0) + 1);
    });
    
    console.log('Edge Analysis:');
    console.log(`  Total edges: ${edges.length}`);
    console.log(`  Unique source nodes: ${edgeSources.size}`);
    console.log(`  Unique target nodes: ${edgeTargets.size}`);
    
    // Find most connected nodes
    const nodeConnections = new Map();
    edges.forEach(([from, to]) => {
        nodeConnections.set(from, (nodeConnections.get(from) || 0) + 1);
        nodeConnections.set(to, (nodeConnections.get(to) || 0) + 1);
    });
    
    const mostConnected = Array.from(nodeConnections.entries())
        .sort(([,a], [,b]) => b - a)
        .slice(0, 5);
    
    console.log('Most Connected Nodes:');
    mostConnected.forEach(([nodeId, connections]) => {
        const node = nodes.find(n => n.id === nodeId);
        console.log(`  ${node ? node.name : nodeId}: ${connections} connections`);
    });
    
    // Analyze script distribution
    console.log('Script Distribution:');
    Array.from(scriptOwners.entries()).forEach(([script, tables]) => {
        console.log(`  ${script}: ${tables.length} tables`);
    });
    
    console.log('=== END ALGORITHM ANALYSIS ===');
    
    return {
        totalNodes: nodes.length,
        volatileNodes: volatileNodes.length,
        globalNodes: globalNodes.length,
        totalEdges: edges.length,
        scriptCount: scriptOwners.size,
        mostConnectedNodes: mostConnected
    };
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
                scripts: {},
                source_file: `${jsonFiles.length}`
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
                        
                        // Create script entry using script_name from the data
                        const scriptName = data.script_name || jsonFile.replace('_lineage.json', '');
                        mergedData.scripts[scriptName] = {
                            script_name: scriptName,
                            tables: data.tables || {},
                            bteq_statements: data.bteq_statements || []
                        };
                        
                        processedFiles++;
                        
                        // When all files are processed, update the display
                        if (processedFiles === totalFiles) {
                            console.log(`Successfully merged ${totalFiles} files`);
                            
                            // Reset network data before loading new data
                            resetNetworkData();
                            
                            lineageData = mergedData;
                            initializeScriptNames();
                            buildOwnershipModel();
                            displaySummary();
                            displayTables();
                            displayStatements();
                            displayNetworkFileGroups();
                            document.getElementById('summarySection').style.display = 'block';
                            document.getElementById('tabSection').style.display = 'block';
                            initializeTableNames();
                            // initializeScriptSearchInputEvents();
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





function showDirectlyRelatedNodes(nodeId) {
    // Find the node in our ownership model
    const node = allNodes[nodeId];
    if (!node) {
        console.error('Node not found in ownership model:', nodeId);
        return;
    }
    
    // Update table filters to show this specific table
    selectedTableFilters = [node.name];
    updateSelectedScriptLabel();
    
    // Use createNetworkVisualization with table filter to show related tables
    createNetworkVisualization(selectedNetworkScript ? [selectedNetworkScript] : [], [node.name]);
}



function showEdgeDetails(edgeId) {
    const networkModal = document.getElementById('networkModal');
    const networkModalTitle = document.getElementById('networkModalTitle');
    const networkModalContent = document.getElementById('networkModalContent');
    
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
    networkModalTitle.textContent = `${fromTable} → ${toTable}`;
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
        let scriptData = null;
        if (lineageData.scripts && lineageData.scripts[group.scriptName]) {
            scriptData = lineageData.scripts[group.scriptName];
        } else if (lineageData.scripts) {
            // Try to find by normalized script name
            for (const [key, sData] of Object.entries(lineageData.scripts)) {
                let normKey = key.split('/').pop().replace(/\.json$/i, '');
                if (normKey.match(/_sh_lineage$/i)) normKey = normKey.replace(/_sh_lineage$/i, '.sh');
                else if (normKey.match(/_ksh_lineage$/i)) normKey = normKey.replace(/_ksh_lineage$/i, '.ksh');
                else if (normKey.match(/_sql_lineage$/i)) normKey = normKey.replace(/_sql_lineage$/i, '.sql');
                else if (normKey.match(/_lineage$/i)) normKey = normKey.replace(/_lineage$/i, '');
                if (normKey === group.scriptName) {
                    scriptData = sData;
                    break;
                }
            }
        }
        group.indices.forEach(localIdx => {
            let sqlStatement = null;
            if (scriptData && scriptData.bteq_statements && scriptData.bteq_statements.length > localIdx) {
                sqlStatement = scriptData.bteq_statements[localIdx];
            } else if (!lineageData.scripts && lineageData.bteq_statements && lineageData.bteq_statements.length > localIdx) {
                sqlStatement = lineageData.bteq_statements[localIdx];
            }
            if (sqlStatement) {
                // Create operation string in new format for clickable link
                const operationString = `${group.scriptName}::op${localIdx}`;
                content += `
                    <div style=\"background: white; padding: 15px; border-radius: 8px; margin-bottom: 15px; border: 1px solid #dee2e6;\">
                        <h5 style=\"color: #007bff; margin-bottom: 10px; cursor: pointer;\" onclick=\"showSql('${operationString}')\">${group.scriptName}:${localIdx}</h5>
                        <div style=\"background: #f8f9fa; padding: 12px; border-radius: 6px; font-family: 'Courier New', monospace; white-space: pre-wrap; font-size: 12px; line-height: 1.4; max-height: 200px; overflow-y: auto; border: 1px solid #e9ecef;\">
${sqlStatement}
                        </div>
                    </div>
                `;
            }
        });
    });
    content += `</div>`;
    networkModalContent.innerHTML = content;
    networkModal.style.display = 'block';
}

function showAllTablesNetwork() {
    selectedNetworkScript = null;
    selectedTableFilters = [];
    updateSelectedScriptLabel();
    // Show all nodes without any filters
    createNetworkVisualization([], []);
}

function showTableNetwork(tableName) {
    // Find the node ID for this table name in our ownership model
    let nodeId = null;
    
    // First, try to find it as a global table
    if (allNodes[tableName]) {
        nodeId = tableName;
    } else {
        // If not found as global, try to find it as a volatile table in any script
        for (const [scriptName, scriptData] of Object.entries(lineageData.scripts)) {
            if (scriptData.tables && scriptData.tables[tableName]) {
                const table = scriptData.tables[tableName];
                if (table.is_volatile) {
                    nodeId = `${scriptName}::${tableName}`;
                    break;
                }
            }
        }
    }
    
    if (nodeId && allNodes[nodeId]) {
        // Show the selected table along with its directly related tables (sources and targets)
        showDirectlyRelatedNodes(nodeId);
    } else {
        console.error('Table not found in ownership model:', tableName);
        // Fallback: show all tables
        selectedTableFilters = [];
        createNetworkVisualization([], []);
        updateSelectedScriptLabel();
    }
}

function showFileNetwork(fileName) {
    console.log('Network script selected:', fileName);
    selectedNetworkScript = fileName;
    selectedTableFilters = [];
    updateSelectedScriptLabel();
    
    // Use the new filtering system
    createNetworkVisualization([fileName], []);
}

// createFilteredNetworkVisualization function removed - replaced by new ownership-based filtering system

function hideSidePanel() {
    const networkModal = document.getElementById('networkModal');
    networkModal.style.display = 'none';
}

function closeNetworkModal() {
    const networkModal = document.getElementById('networkModal');
    networkModal.style.display = 'none';
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
    const networkModal = document.getElementById('networkModal');
    const networkStatsModal = document.getElementById('networkStatsModal');
    
    if (event.target === sqlModal) {
        closeSqlModal();
    }
    
    if (event.target === networkModal) {
        closeNetworkModal();
    }
    
    if (event.target === networkStatsModal) {
        closeNetworkStatsModal();
    }
}

// Global click and keyboard handlers are now handled by the consolidated event system

// Search/filter for network node by name

function initializeTableNames() {
    if (allNodes) {
        allTableNames = Object.values(allNodes).map(node => node.name).sort();
    }
}



function searchNetworkNode() {
    const input = document.getElementById('networkNodeSearchInput');
    const query = input.value.trim().toLowerCase();
    if (!query) {
        if (selectedNetworkScript) {
            // Show all tables from the selected script
            createNetworkVisualization([selectedNetworkScript], []);
        } else {
            // Show all tables
            createNetworkVisualization([], []);
        }
        selectedTableFilters = [];
        updateSelectedScriptLabel();
        return;
    }
    
    // Find all nodes that match (case-insensitive, partial match)
    const matchedNodes = Object.values(allNodes).filter(node => 
        node.name.toLowerCase().includes(query)
    );
    
    if (matchedNodes.length === 1) {
        // Single match - show directly related nodes
        showDirectlyRelatedNodes(matchedNodes[0].id);
        hideAutocompleteDropdown();
    } else if (matchedNodes.length > 1) {
        // Multiple matches - filter by table names
        const tableNames = matchedNodes.map(node => node.name);
        selectedTableFilters = tableNames;
        createNetworkVisualization(selectedNetworkScript ? [selectedNetworkScript] : [], tableNames);
        updateSelectedScriptLabel();
        hideAutocompleteDropdown();
    } else {
        // Show empty network with a message
        const container = document.getElementById('networkContainer');
        container.innerHTML = '<div style="padding: 40px; color: #dc3545; text-align: center; font-size: 1.2em;">No table found matching that name.</div>';
        if (network) {
            network.destroy();
            network = null;
        }
        selectedTableFilters = [];
        updateSelectedScriptLabel();
    }
}

function clearNetworkNodeSearch() {
    document.getElementById('networkNodeSearchInput').value = '';
    selectedTableFilters = [];
    // Keep the current script filter, only clear table filters
    createNetworkVisualization(
        selectedNetworkScript ? [selectedNetworkScript] : [], 
        []
    );
    updateSelectedScriptLabel();
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

// Table autocomplete events are now handled by the consolidated event system

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
                scripts: {},
                source_file: `${jsonFiles.length}`
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
                        
                        // Create script entry using script_name from the data
                        const scriptName = data.script_name || jsonFile.replace('_lineage.json', '');
                        mergedData.scripts[scriptName] = {
                            script_name: scriptName,
                            tables: data.tables || {},
                            bteq_statements: data.bteq_statements || []
                        };
                        
                        processedFiles++;
                        
                        // When all files are processed, update the display
                        if (processedFiles === totalFiles) {
                            console.log(`Successfully merged ${totalFiles} files from ${folderPath}`);
                            
                            // Reset network data before loading new data
                            resetNetworkData();
                            
                            lineageData = mergedData;
                            initializeScriptNames();
                            buildOwnershipModel();
                            displaySummary();
                            displayTables();
                            displayStatements();
                            displayNetworkFileGroups();
                            document.getElementById('summarySection').style.display = 'block';
                            document.getElementById('tabSection').style.display = 'block';
                            initializeTableNames();
                            // initializeScriptSearchInputEvents();
                            
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
    
    let filterText = 'Filter: ';
    let hasFilters = false;
    
    // Add script filter
    if (selectedNetworkScript && lineageData.scripts && lineageData.scripts[selectedNetworkScript]) {
        filterText += `Scripts: [${selectedNetworkScript}]`;
        hasFilters = true;
    }
    
    // Add table filter
    if (selectedTableFilters && selectedTableFilters.length > 0) {
        if (hasFilters) {
            filterText += ', ';
        }
        filterText += `Tables: [${selectedTableFilters.join(', ')}]`;
        hasFilters = true;
    }
    
    // Add connection mode
    if (connectionMode !== 'direct') {
        if (hasFilters) {
            filterText += ', ';
        }
        filterText += `Mode: ${connectionMode}`;
        hasFilters = true;
    }
    
    // Add flow view status
    if (flowViewEnabled) {
        if (hasFilters) {
            filterText += ', ';
        }
        filterText += 'Flow View: ON';
        hasFilters = true;
    }
    
    if (hasFilters) {
        labelDiv.textContent = filterText;
    } else {
        labelDiv.textContent = 'Filter: None';
    }
    labelDiv.style.display = '';
}

// Apply filters to the ownership model
function applyFilters(scriptFilters = [], tableFilters = [], mode = 'direct') {
    let filteredNodes = Object.entries(allNodes);
    let relatedNodeIds = null;
    
    // 1. Apply script filters first
    if (scriptFilters.length > 0) {
        filteredNodes = filteredNodes.filter(([nodeId, node]) => {
            const hasMatchingOwner = node.owners.some(owner => 
                scriptFilters.includes(owner)
            );
            return hasMatchingOwner;
        });
    }
    
    // 2. Apply table filters - show tables that match the filter AND their connected tables
    if (tableFilters.length > 0) {
        // First, find all nodes that match the table filter (search in ALL nodes, not just filtered ones)
        const matchingNodeIds = new Set();
        relatedNodeIds = new Set();
        
        // Add nodes that match the table filter from ALL nodes
        Object.entries(allNodes).forEach(([nodeId, node]) => {
            if (tableFilters.includes(node.name)) {
                matchingNodeIds.add(nodeId);
                relatedNodeIds.add(nodeId);
            }
        });
        
        if (mode === 'impacts_by' || mode === 'impacted_by' || mode === 'both') {
            // Step 1: Find all nodes that the selected nodes can reach (downstream)
            const downstreamNodes = new Set();
            const downstreamEdges = new Set();
            
            if (mode === 'impacts_by' || mode === 'both') {
                const visited = new Set();
                const queue = Array.from(matchingNodeIds);
                
                while (queue.length > 0) {
                    const currentNodeId = queue.shift();
                    
                    if (visited.has(currentNodeId)) {
                        continue;
                    }
                    
                    visited.add(currentNodeId);
                    downstreamNodes.add(currentNodeId);
                    
                    // Find all edges that start from this node (downstream)
                    allEdges.forEach(([from, to, operations]) => {
                        if (from === currentNodeId && !visited.has(to)) {
                            queue.push(to);
                            downstreamEdges.add(`${from}->${to}`);
                        }
                    });
                }
            }
            
            // Step 2: Find all nodes that can reach the selected nodes (upstream)
            const upstreamNodes = new Set();
            const upstreamEdges = new Set();
            
            if (mode === 'impacted_by' || mode === 'both') {
                const visited = new Set();
                const queue = Array.from(matchingNodeIds);
                
                while (queue.length > 0) {
                    const currentNodeId = queue.shift();
                    
                    if (visited.has(currentNodeId)) {
                        continue;
                    }
                    
                    visited.add(currentNodeId);
                    upstreamNodes.add(currentNodeId);
                    
                    // Find all edges that end at this node (upstream)
                    allEdges.forEach(([from, to, operations]) => {
                        if (to === currentNodeId && !visited.has(from)) {
                            queue.push(from);
                            upstreamEdges.add(`${from}->${to}`);
                        }
                    });
                }
            }
            
            // Combine nodes based on mode
            if (mode === 'impacts_by') {
                downstreamNodes.forEach(nodeId => relatedNodeIds.add(nodeId));
            } else if (mode === 'impacted_by') {
                upstreamNodes.forEach(nodeId => relatedNodeIds.add(nodeId));
            } else if (mode === 'both') {
                downstreamNodes.forEach(nodeId => relatedNodeIds.add(nodeId));
                upstreamNodes.forEach(nodeId => relatedNodeIds.add(nodeId));
            }
            
            // Log the analysis for debugging
            console.log('Graph analysis:', {
                mode,
                selectedNodes: Array.from(matchingNodeIds),
                downstreamNodes: Array.from(downstreamNodes),
                upstreamNodes: Array.from(upstreamNodes),
                totalRelated: relatedNodeIds.size,
                downstreamEdges: Array.from(downstreamEdges),
                upstreamEdges: Array.from(upstreamEdges)
            });
            
            // Additional debug info for each mode
            if (mode === 'impacts_by') {
                console.log(`Mode 'impacts_by': Showing ${downstreamNodes.size} nodes that the selected nodes can reach`);
            } else if (mode === 'impacted_by') {
                console.log(`Mode 'impacted_by': Showing ${upstreamNodes.size} nodes that can reach the selected nodes`);
            } else if (mode === 'both') {
                console.log(`Mode 'both': Showing ${downstreamNodes.size} downstream + ${upstreamNodes.size} upstream nodes`);
            }
        } else {
            // Direct mode: only add directly connected tables (sources and targets)
            // Use edges to find directly connected nodes in both directions
            allEdges.forEach(([from, to, operations]) => {
                if (matchingNodeIds.has(from)) {
                    // If the source node matches, add the target node
                    relatedNodeIds.add(to);
                }
                if (matchingNodeIds.has(to)) {
                    // If the target node matches, add the source node
                    relatedNodeIds.add(from);
                }
            });
        }
        
        // Now filter nodes to only include the matching and related nodes
        // But also respect script filters if they were applied
        filteredNodes = Object.entries(allNodes).filter(([nodeId, node]) => {
            // Must be in the related set
            if (!relatedNodeIds.has(nodeId)) {
                return false;
            }
            
            // If script filters are applied, must also match script filter
            if (scriptFilters.length > 0) {
                const hasMatchingOwner = node.owners.some(owner => 
                    scriptFilters.includes(owner)
                );
                return hasMatchingOwner;
            }
            
            return true;
        });
    }
    
    // 3. Filter edges based on filtered nodes and filter operations by script
    const nodeIds = new Set(filteredNodes.map(([id, _]) => id));
    const filteredEdges = allEdges
        .filter(([from, to, operations]) => 
            nodeIds.has(from) && nodeIds.has(to)
        )
        .map(([from, to, operations]) => {
            // If script filters are applied, filter operations by script name
            if (scriptFilters.length > 0) {
                const filteredOperations = operations.filter(op => {
                    const scriptName = op.split('::')[0];
                    return scriptFilters.includes(scriptName);
                });
                return [from, to, filteredOperations];
            }
            return [from, to, operations];
        })
        .filter(([from, to, operations]) => {
            // Remove edges that have no operations after filtering
            if (!operations || operations.length === 0) {
                console.log(`Filtering out edge with no operations: ${from} -> ${to}`);
                return false;
            }
            return true;
        });
    
    console.log('Filter applied:', {
        scriptFilters,
        tableFilters,
        mode,
        filteredNodes: filteredNodes.length,
        filteredEdges: filteredEdges.length,
        relatedNodeIds: relatedNodeIds ? relatedNodeIds.size : 0
    });
    
    return { 
        nodes: filteredNodes.map(([id, node]) => ({ id, ...node })), 
        edges: filteredEdges 
    };
}

// Add script search for network view
function searchNetworkScript() {
    const input = document.getElementById('networkScriptSearchInput');
    const query = input.value.trim();
    if (!query) {
        // If empty, show all scripts
        selectedNetworkScript = null;
        selectedTableFilters = [];
        updateSelectedScriptLabel();
        createNetworkVisualization([], []);
        return;
    }
    // Find exact match (case-insensitive) in script names
    const scriptNames = Object.keys(lineageData && lineageData.scripts ? lineageData.scripts : {});
    const match = scriptNames.find(name => name.toLowerCase() === query.toLowerCase());
    if (match) {
        selectedNetworkScript = match;
        selectedTableFilters = [];
        updateSelectedScriptLabel();
        createNetworkVisualization([match], []);
    } else {
        // No match, show error or do nothing
        alert('No script found matching that name.');
    }
}

function clearNetworkScriptSearch() {
    document.getElementById('networkScriptSearchInput').value = '';
    selectedNetworkScript = null;
    // Keep the current table filters, only clear script filter
    createNetworkVisualization([], selectedTableFilters);
    updateSelectedScriptLabel();
}


// --- Script autocomplete state ---

// Update allScriptNames when data is loaded
function initializeScriptNames() {
    if (lineageData && lineageData.scripts) {
        allScriptNames = Object.keys(lineageData.scripts).sort();
    } else {
        allScriptNames = [];
    }
}

function updateScriptAutocompleteDropdown() {
    const input = document.getElementById('networkScriptSearchInput');
    const dropdown = document.getElementById('scriptAutocompleteDropdown');
    if (!input || !dropdown) return;
    const query = input.value.trim().toLowerCase();
    if (!query) {
        dropdown.style.display = 'none';
        filteredScriptNames = [];
        return;
    }
    filteredScriptNames = allScriptNames.filter(name => name.toLowerCase().includes(query));
    if (filteredScriptNames.length === 0) {
        dropdown.style.display = 'none';
        return;
    }
    dropdown.innerHTML = filteredScriptNames.map((name, idx) =>
        `<div class="autocomplete-item${idx === selectedScriptAutocompleteIndex ? ' selected' : ''}" onclick="selectScriptAutocompleteItem(${idx})">${name}</div>`
    ).join('');
    dropdown.style.display = 'block';
}

function showScriptAutocompleteDropdown() {
    const dropdown = document.getElementById('scriptAutocompleteDropdown');
    if (filteredScriptNames.length > 0) {
        dropdown.style.display = 'block';
    } else {
        dropdown.style.display = 'none';
    }
}

function hideScriptAutocompleteDropdown() {
    const dropdown = document.getElementById('scriptAutocompleteDropdown');
    dropdown.style.display = 'none';
    selectedScriptAutocompleteIndex = -1;
}

function selectScriptAutocompleteItem(index) {
    if (index >= 0 && index < filteredScriptNames.length) {
        const selectedScript = filteredScriptNames[index];
        document.getElementById('networkScriptSearchInput').value = selectedScript;
        hideScriptAutocompleteDropdown();
        showFileNetwork(selectedScript);
    } else {
        console.log('Invalid index or no filtered scripts available');
    }
}

function navigateScriptAutocomplete(direction) {
    if (filteredScriptNames.length === 0) return;
    if (direction === 'up') {
        selectedScriptAutocompleteIndex = selectedScriptAutocompleteIndex <= 0 ? filteredScriptNames.length - 1 : selectedScriptAutocompleteIndex - 1;
    } else if (direction === 'down') {
        selectedScriptAutocompleteIndex = selectedScriptAutocompleteIndex >= filteredScriptNames.length - 1 ? 0 : selectedScriptAutocompleteIndex + 1;
    }
    updateScriptAutocompleteDropdown();
}

// --- Consolidated Event Handling System ---
// Single DOMContentLoaded event listener to handle all initialization
window.addEventListener('DOMContentLoaded', function() {
    console.log('Initializing consolidated event handling system...');
    
    // Initialize fullscreen functionality
    console.log('Fullscreen functionality initialized');
    
    // Initialize table autocomplete events
    initializeTableAutocompleteEvents();
    
    // Initialize script autocomplete events  
    initializeScriptAutocompleteEvents();
    
    // Initialize global click handlers
    initializeGlobalClickHandlers();
    
    // Initialize global keyboard handlers
    initializeGlobalKeyboardHandlers();
    
    // Initialize zoom keyboard shortcuts
    initializeZoomKeyboardShortcuts();
});

function initializeTableAutocompleteEvents() {
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
    }
}

function initializeScriptAutocompleteEvents() {
    const input = document.getElementById('networkScriptSearchInput');
    if (input) {
        input.addEventListener('input', function() {
            updateScriptAutocompleteDropdown();
        });
        
        input.addEventListener('keydown', function(e) {
            if (e.key === 'Enter') {
                if (selectedScriptAutocompleteIndex >= 0) {
                    selectScriptAutocompleteItem(selectedScriptAutocompleteIndex);
                } else {
                    searchNetworkScript();
                }
                e.preventDefault();
            } else if (e.key === 'ArrowUp') {
                navigateScriptAutocomplete('up');
                e.preventDefault();
            } else if (e.key === 'ArrowDown') {
                navigateScriptAutocomplete('down');
                e.preventDefault();
            } else if (e.key === 'Escape') {
                hideScriptAutocompleteDropdown();
                e.preventDefault();
            }
        });
    }
}

function initializeGlobalClickHandlers() {
    // Single consolidated click handler for all dropdown hiding
    document.addEventListener('click', function(e) {
        // Hide script autocomplete if click is outside its container
        if (!e.target.closest('#networkScriptSearchInput') && !e.target.closest('#scriptAutocompleteDropdown')) {
            hideScriptAutocompleteDropdown();
        }
        
        // Hide table autocomplete if click is outside its container
        if (!e.target.closest('#networkNodeSearchInput') && !e.target.closest('#autocompleteDropdown')) {
            hideAutocompleteDropdown();
        }
        
        // Handle deselection when clicking on empty areas in content areas
        const contentArea = document.getElementById('contentArea');
        const statementContentArea = document.getElementById('statementContentArea');
        
        if (contentArea && contentArea.contains(e.target) && 
            !e.target.closest('.table-item, .tree-item, .statement-item, .relationship-item, .operation-badge')) {
            deselectAll();
        }
        
        if (statementContentArea && statementContentArea.contains(e.target) && 
            !e.target.closest('.table-item, .tree-item, .statement-item, .relationship-item, .operation-badge')) {
            deselectAll();
        }
    });
}

function initializeGlobalKeyboardHandlers() {
    // Single consolidated keyboard handler
    document.addEventListener('keydown', function(event) {
        // Escape key to deselect all
        if (event.key === 'Escape') {
            deselectAll();
        }
    });
}



function toggleConnectionMode() {
    const select = document.getElementById('connectionModeSelect');
    
    // Update the connection mode based on dropdown selection
    connectionMode = select.value;
    
    // Re-render the network with the new mode (will only recreate if filters changed)
    createNetworkVisualization(
        selectedNetworkScript ? [selectedNetworkScript] : [], 
        selectedTableFilters
    );
}

function toggleLockView() {
    const checkbox = document.getElementById('lockViewCheckbox');
    
    // Update the lock view state based on checkbox state
    lockViewEnabled = checkbox.checked;
    
    console.log('Lock view:', lockViewEnabled ? 'enabled' : 'disabled');
}

function toggleFlowView() {
    const checkbox = document.getElementById('flowViewCheckbox');
    
    // Update the flow view state based on checkbox state
    flowViewEnabled = checkbox.checked;
    
    console.log('Flow view:', flowViewEnabled ? 'enabled' : 'disabled');
    
    // Re-render the network with the new layout
    createNetworkVisualization(
        selectedNetworkScript ? [selectedNetworkScript] : [], 
        selectedTableFilters,
        true // force recreation
    );
    
    // Update the selected script label to show flow view status
    updateSelectedScriptLabel();
}

// Removed duplicate initializeScriptSearchInputEvents function - functionality is now handled by initializeScriptAutocompleteEvents()

// Fullscreen functionality
let isFullscreen = false;

function toggleFullscreen() {
    const container = document.getElementById('networkFullscreenContainer');
    const toggleBtn = document.getElementById('fullscreenToggleBtn');
    
    if (!isFullscreen) {
        // Enter fullscreen
        container.classList.add('fullscreen');
        toggleBtn.innerHTML = '❌';
        toggleBtn.title = 'Exit fullscreen mode';
        isFullscreen = true;
        
        // Recreate network to fit new container size
        setTimeout(() => {
            forceNetworkRecreation();
            // Ensure zoom buttons are added after network recreation
            addZoomButtons();
        }, 100);
        
        // Add escape key listener
        document.addEventListener('keydown', handleFullscreenEscape);
        
    } else {
        // Exit fullscreen
        container.classList.remove('fullscreen');
        toggleBtn.innerHTML = '🔍';
        toggleBtn.title = 'Toggle fullscreen mode';
        isFullscreen = false;
        
        // Recreate network to fit original container size
        setTimeout(() => {
            forceNetworkRecreation();
            // Ensure zoom buttons are added after network recreation
            addZoomButtons();
        }, 100);
        
        // Remove escape key listener
        document.removeEventListener('keydown', handleFullscreenEscape);
    }
}

function handleFullscreenEscape(event) {
    if (event.key === 'Escape' && isFullscreen) {
        toggleFullscreen();
    }
}


// Helper function to force network recreation (useful for container size changes)
function forceNetworkRecreation() {
    createNetworkVisualization(
        selectedNetworkScript ? [selectedNetworkScript] : [], 
        selectedTableFilters,
        true // force recreation
    );
}

// Network Statistics Functions
function showNetworkStatistics() {
    const modal = document.getElementById('networkStatsModal');
    const content = document.getElementById('networkStatsModalContent');
    
    // Show loading state
    content.innerHTML = '<div class="stats-loading">Calculating network statistics...</div>';
    modal.style.display = 'block';
    
    // Calculate statistics asynchronously
    setTimeout(() => {
        const stats = calculateNetworkStatistics();
        const { nodes: filteredNodes } = applyFilters(
            selectedNetworkScript ? [selectedNetworkScript] : [], 
            selectedTableFilters, 
            connectionMode
        );
        displayNetworkStatistics(stats, filteredNodes);
    }, 100);
}

function closeNetworkStatsModal() {
    const modal = document.getElementById('networkStatsModal');
    modal.style.display = 'none';
}

function calculateNetworkStatistics() {
    // Get current filtered data
    const { nodes: filteredNodes, edges: filteredEdges } = applyFilters(
        selectedNetworkScript ? [selectedNetworkScript] : [], 
        selectedTableFilters, 
        connectionMode
    );
    
    // Calculate basic statistics
    const totalTables = filteredNodes.length;
    const totalEdges = filteredEdges.length;
    
    // Find source tables (tables with no incoming edges)
    const sourceTables = filteredNodes.filter(node => {
        const hasIncomingEdges = getIncomingEdges(node.id, filteredEdges).length > 0;
        return !hasIncomingEdges;
    });
    
    // Find final target tables (tables with no outgoing edges or only self-referencing edges)
    const finalTargetTables = filteredNodes.filter(node => {
        const outgoingEdges = getOutgoingEdges(node.id, filteredEdges);
        
        // If no outgoing edges, it's a final table
        if (outgoingEdges.length === 0) {
            return true;
        }
        
        // If all outgoing edges are self-referencing (from === to), it's a final table
        const hasOnlySelfReferences = outgoingEdges.every(([from, to]) => from === to);
        return hasOnlySelfReferences;
    });
    
    // Find unused volatile tables (volatile tables with no targets)
    const unusedVolatileTables = filteredNodes.filter(node => {
        if (!node.is_volatile) return false;
        const hasOutgoingEdges = getOutgoingEdges(node.id, filteredEdges).length > 0;
        return !hasOutgoingEdges;
    });
    
    // Calculate table types
    const volatileTables = filteredNodes.filter(node => node.is_volatile);
    const globalTables = filteredNodes.filter(node => !node.is_volatile);
    
    // Calculate average connections
    // Exclude self-referencing edges from connection count
    const nonSelfReferencingEdges = filterNonSelfReferencingEdges(filteredEdges);
    const totalConnections = nonSelfReferencingEdges.length * 2; // Each edge connects 2 nodes
    const avgConnections = totalTables > 0 ? (totalConnections / totalTables).toFixed(1) : 0;
    
    // Find most connected tables
    const nodeConnections = {};
    filteredNodes.forEach(node => {
        nodeConnections[node.id] = 0;
    });
    
    filterNonSelfReferencingEdges(filteredEdges).forEach(([from, to]) => {
        nodeConnections[from] = (nodeConnections[from] || 0) + 1;
        nodeConnections[to] = (nodeConnections[to] || 0) + 1;
    });
    
    const mostConnectedTables = Object.entries(nodeConnections)
        .sort(([,a], [,b]) => b - a)
        .map(([nodeId, connections]) => {
            const node = filteredNodes.find(n => n.id === nodeId);
            return {
                name: node.name,
                connections: connections,
                isVolatile: node.is_volatile,
                owners: node.owners
            };
        });
    
    // Extract scripts from operations in the network
    const includedScripts = new Set();
    const scriptOperationCounts = {};
    
    filteredEdges.forEach(([from, to, operations]) => {
        if (operations && operations.length > 0) {
            operations.forEach(op => {
                const [scriptName, opId] = op.split('::');
                if (scriptName) {
                    includedScripts.add(scriptName);
                    scriptOperationCounts[scriptName] = (scriptOperationCounts[scriptName] || 0) + 1;
                }
            });
        }
    });
    
    // Convert to array and sort by operation count
    const includedScriptsList = Array.from(includedScripts).map(scriptName => ({
        name: scriptName,
        operationCount: scriptOperationCounts[scriptName] || 0
    })).sort((a, b) => b.operationCount - a.operationCount);
    
    return {
        totalTables,
        totalEdges,
        sourceTables: sourceTables.length,
        finalTargetTables: finalTargetTables.length,
        unusedVolatileTables: unusedVolatileTables.length,
        volatileTables: volatileTables.length,
        globalTables: globalTables.length,
        avgConnections,
        mostConnectedTables,
        includedScripts: includedScriptsList,
        unusedVolatileTableDetails: unusedVolatileTables.map(node => ({
            name: node.name,
            owners: node.owners
        })),
        sourceTableDetails: sourceTables.map(node => ({
            name: node.name,
            isVolatile: node.is_volatile,
            owners: node.owners
        })),
        finalTargetTableDetails: finalTargetTables.map(node => ({
            name: node.name,
            isVolatile: node.is_volatile,
            owners: node.owners
        })),
        currentFilters: {
            scriptFilters: selectedNetworkScript ? [selectedNetworkScript] : [],
            tableFilters: selectedTableFilters,
            mode: connectionMode
        }
    };
}

function displayNetworkStatistics(stats, filteredNodes) {
    const content = document.getElementById('networkStatsModalContent');
    
    let html = `
        <div class="filter-info">
            <h4>📊 Current Filters</h4>
            <div class="filter-item">
                <span class="filter-label">Script Filters:</span>
                <span class="filter-value">${stats.currentFilters.scriptFilters.length > 0 ? stats.currentFilters.scriptFilters.join(', ') : 'None'}</span>
            </div>
            <div class="filter-item">
                <span class="filter-label">Table Filters:</span>
                <span class="filter-value">${stats.currentFilters.tableFilters.length > 0 ? stats.currentFilters.tableFilters.join(', ') : 'None'}</span>
            </div>
            <div class="filter-item">
                <span class="filter-label">Connection Mode:</span>
                <span class="filter-value">${stats.currentFilters.mode === 'direct' ? 'Direct' : 'Indirect'}</span>
            </div>
        </div>
        
        <div class="stats-grid">
            <div class="stat-card">
                <h4>Total Tables</h4>
                <div class="stat-number">${stats.totalTables}</div>
                <div class="stat-description">Tables in current view</div>
            </div>
            
            <div class="stat-card">
                <h4>Total Connections</h4>
                <div class="stat-number">${stats.totalEdges}</div>
                <div class="stat-description">Data flow connections</div>
            </div>
            
            <div class="stat-card">
                <h4>Source Tables</h4>
                <div class="stat-number">${stats.sourceTables}</div>
                <div class="stat-description">Tables with no incoming data</div>
            </div>
            
            <div class="stat-card">
                <h4>Final Tables</h4>
                <div class="stat-number">${stats.finalTargetTables}</div>
                <div class="stat-description">Tables with no outgoing data or only self-references</div>
            </div>
            
            <div class="stat-card">
                <h4>Volatile Tables</h4>
                <div class="stat-number">${stats.volatileTables}</div>
                <div class="stat-description">Script-specific tables</div>
            </div>
            
            <div class="stat-card">
                <h4>Global Tables</h4>
                <div class="stat-number">${stats.globalTables}</div>
                <div class="stat-description">Shared across scripts</div>
            </div>
            
            <div class="stat-card">
                <h4>Avg Connections</h4>
                <div class="stat-number">${stats.avgConnections}</div>
                <div class="stat-description">Per table average</div>
            </div>
            
            <div class="stat-card">
                <h4>Unused Volatile</h4>
                <div class="stat-number">${stats.unusedVolatileTables}</div>
                <div class="stat-description">Should be removed</div>
            </div>
            
            <div class="stat-card">
                <h4>Included Scripts</h4>
                <div class="stat-number">${stats.includedScripts.length}</div>
                <div class="stat-description">Scripts with operations</div>
            </div>
        </div>
    `;
    
    // Add Scripts section
    if (stats.includedScripts.length > 0) {
        html += `
            <div style="background: white; padding: 20px; border-radius: 8px; border: 1px solid #dee2e6; margin-bottom: 20px;">
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px; cursor: pointer;" onclick="toggleStatsSection('included-scripts')">
                    <h4 style="color: #495057; margin: 0;">📜 Included Scripts (${stats.includedScripts.length})</h4>
                    <span id="included-scripts-toggle" style="font-size: 18px; color: #6c757d;">▼</span>
                </div>
                <div id="included-scripts-content" style="display: block;">
                    <p style="color: #6c757d; margin-bottom: 15px; font-size: 0.9em;">
                        Scripts that have operations in the current network view.
                    </p>
                    ${stats.includedScripts.map(script => `
                        <div style="display: flex; justify-content: space-between; align-items: center; padding: 8px 0; border-bottom: 1px solid #f0f0f0; cursor: pointer; transition: background-color 0.2s;" 
                             onmouseover="this.style.backgroundColor='#f8f9fa'" 
                             onmouseout="this.style.backgroundColor='transparent'"
                             onclick="showScriptOperations('${script.name}')">
                            <div>
                                <span style="font-weight: bold; color: #495057;">${script.name}</span>
                            </div>
                            <div style="text-align: right;">
                                <div style="font-weight: bold; color: #28a745;">${script.operationCount} operations</div>
                            </div>
                        </div>
                    `).join('')}
                </div>
            </div>
        `;
    }
    
    // Add Tables section
    if (stats.mostConnectedTables.length > 0) {
        html += `
            <div style="background: white; padding: 20px; border-radius: 8px; border: 1px solid #dee2e6; margin-bottom: 20px;">
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px; cursor: pointer;" onclick="toggleStatsSection('most-connected-tables')">
                    <h4 style="color: #495057; margin: 0;">Tables (${stats.mostConnectedTables.length})</h4>
                    <span id="most-connected-tables-toggle" style="font-size: 18px; color: #6c757d;">▼</span>
                </div>
                <div id="most-connected-tables-content" style="display: block;">
                    ${stats.mostConnectedTables.map(table => `
                        <div style="display: flex; justify-content: space-between; align-items: center; padding: 8px 0; border-bottom: 1px solid #f0f0f0;">
                            <div>
                                <span style="font-weight: bold; color: #495057;">${table.name}</span>
                                ${table.isVolatile ? '<span style="background: #ffc107; color: #856404; padding: 2px 6px; border-radius: 4px; font-size: 0.7em; margin-left: 8px;">VOLATILE</span>' : ''}
                            </div>
                            <div style="text-align: right;">
                                <div style="font-weight: bold; color: #007bff;">${table.connections} connections</div>
                                <div style="font-size: 0.8em; color: #6c757d;">${table.owners.join(', ')}</div>
                            </div>
                        </div>
                    `).join('')}
                </div>
            </div>
        `;
    }
    
    // Add unused volatile tables section
    if (stats.unusedVolatileTableDetails.length > 0) {
        html += `
            <div class="unused-tables-section">
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px; cursor: pointer;" onclick="toggleStatsSection('unused-volatile-tables')">
                    <h4 style="margin: 0;">⚠️ Unused Volatile Tables (${stats.unusedVolatileTableDetails.length})</h4>
                    <span id="unused-volatile-tables-toggle" style="font-size: 18px; color: #6c757d;">▼</span>
                </div>
                <div id="unused-volatile-tables-content" style="display: block;">
                    <p style="color: #856404; margin-bottom: 15px; font-size: 0.9em;">
                        These volatile tables have no targets and should be removed from the scripts.
                    </p>
                    ${stats.unusedVolatileTableDetails.map(table => `
                        <div class="unused-table-item">
                            <span class="table-name">${table.name}</span>
                            <span class="table-owner">${table.owners.join(', ')}</span>
                        </div>
                    `).join('')}
                </div>
            </div>
        `;
    } else {
        html += `
            <div class="unused-tables-section">
                <h4>✅ No Unused Volatile Tables</h4>
                <div class="no-unused-tables">
                    All volatile tables have targets. Great job!
                </div>
            </div>
        `;
    }
    
    // Add source and target table details
    if (stats.sourceTableDetails.length > 0) {
        html += `
            <div style="background: white; padding: 20px; border-radius: 8px; border: 1px solid #dee2e6; margin-top: 20px;">
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px; cursor: pointer;" onclick="toggleStatsSection('source-tables')">
                    <h4 style="color: #495057; margin: 0;">📥 Source Tables (${stats.sourceTableDetails.length})</h4>
                    <span id="source-tables-toggle" style="font-size: 18px; color: #6c757d;">▼</span>
                </div>
                <div id="source-tables-content" style="display: block;">
                    <p style="color: #6c757d; margin-bottom: 15px; font-size: 0.9em;">
                        Tables that provide data but don't receive data from other tables.
                    </p>
                    ${stats.sourceTableDetails.map(table => `
                        <div style="display: flex; justify-content: space-between; align-items: center; padding: 8px 0; border-bottom: 1px solid #f0f0f0;">
                            <div>
                                <span style="font-weight: bold; color: #495057;">${table.name}</span>
                                ${table.isVolatile ? '<span style="background: #ffc107; color: #856404; padding: 2px 6px; border-radius: 4px; font-size: 0.7em; margin-left: 8px;">VOLATILE</span>' : ''}
                            </div>
                            <div style="font-size: 0.8em; color: #6c757d; font-style: italic;">
                                ${table.owners.join(', ')}
                            </div>
                        </div>
                    `).join('')}
                </div>
            </div>
        `;
    }
    
    if (stats.finalTargetTableDetails.length > 0) {
        html += `
            <div style="background: white; padding: 20px; border-radius: 8px; border: 1px solid #dee2e6; margin-top: 20px;">
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px; cursor: pointer;" onclick="toggleStatsSection('final-tables')">
                    <h4 style="color: #495057; margin: 0;">📤 Final Tables (${stats.finalTargetTableDetails.length})</h4>
                    <span id="final-tables-toggle" style="font-size: 18px; color: #6c757d;">▼</span>
                </div>
                <div id="final-tables-content" style="display: block;">
                    <p style="color: #6c757d; margin-bottom: 15px; font-size: 0.9em;">
                        Tables that receive data but don't provide data to other tables, or only reference themselves.
                    </p>
                    ${stats.finalTargetTableDetails.map(table => `
                        <div style="display: flex; justify-content: space-between; align-items: center; padding: 8px 0; border-bottom: 1px solid #f0f0f0;">
                            <div>
                                <span style="font-weight: bold; color: #495057;">${table.name}</span>
                                ${table.isVolatile ? '<span style="background: #ffc107; color: #856404; padding: 2px 6px; border-radius: 4px; font-size: 0.7em; margin-left: 8px;">VOLATILE</span>' : ''}
                            </div>
                            <div style="font-size: 0.8em; color: #6c757d; font-style: italic;">
                                ${table.owners.join(', ')}
                            </div>
                        </div>
                    `).join('')}
                </div>
            </div>
        `;
    }
    
    // Add global tables section
    if (stats.globalTables > 0) {
        const globalTableDetails = filteredNodes.filter(node => !node.is_volatile).map(node => ({
            name: node.name,
            owners: node.owners
        }));
        
        html += `
            <div style="background: white; padding: 20px; border-radius: 8px; border: 1px solid #dee2e6; margin-top: 20px;">
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px; cursor: pointer;" onclick="toggleStatsSection('global-tables')">
                    <h4 style="color: #495057; margin: 0;">🌐 Global Tables (${stats.globalTables})</h4>
                    <span id="global-tables-toggle" style="font-size: 18px; color: #6c757d;">▼</span>
                </div>
                <div id="global-tables-content" style="display: block;">
                    <p style="color: #6c757d; margin-bottom: 15px; font-size: 0.9em;">
                        Tables shared across multiple scripts. These are persistent and can be referenced by any script.
                    </p>
                    ${globalTableDetails.map(table => `
                        <div style="display: flex; justify-content: space-between; align-items: center; padding: 8px 0; border-bottom: 1px solid #f0f0f0;">
                            <div>
                                <span style="font-weight: bold; color: #495057;">${table.name}</span>
                            </div>
                            <div style="font-size: 0.8em; color: #6c757d; font-style: italic;">
                                ${table.owners.join(', ')}
                            </div>
                        </div>
                    `).join('')}
                </div>
            </div>
        `;
    }
    
    // Add volatile tables section
    if (stats.volatileTables > 0) {
        const volatileTableDetails = filteredNodes.filter(node => node.is_volatile).map(node => ({
            name: node.name,
            owners: node.owners
        }));
        
        html += `
            <div style="background: white; padding: 20px; border-radius: 8px; border: 1px solid #dee2e6; margin-top: 20px;">
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px; cursor: pointer;" onclick="toggleStatsSection('volatile-tables')">
                    <h4 style="color: #495057; margin: 0;">⚡ Volatile Tables (${stats.volatileTables})</h4>
                    <span id="volatile-tables-toggle" style="font-size: 18px; color: #6c757d;">▼</span>
                </div>
                <div id="volatile-tables-content" style="display: block;">
                    <p style="color: #6c757d; margin-bottom: 15px; font-size: 0.9em;">
                        Script-specific temporary tables. These are created and destroyed within individual scripts.
                    </p>
                    ${volatileTableDetails.map(table => `
                        <div style="display: flex; justify-content: space-between; align-items: center; padding: 8px 0; border-bottom: 1px solid #f0f0f0;">
                            <div>
                                <span style="font-weight: bold; color: #495057;">${table.name}</span>
                                <span style="background: #ffc107; color: #856404; padding: 2px 6px; border-radius: 4px; font-size: 0.7em; margin-left: 8px;">VOLATILE</span>
                            </div>
                            <div style="font-size: 0.8em; color: #6c757d; font-style: italic;">
                                ${table.owners.join(', ')}
                            </div>
                        </div>
                    `).join('')}
                </div>
            </div>
        `;
    }
    
    content.innerHTML = html;
}

// Function to toggle fold/unfold for statistics sections
function toggleStatsSection(sectionId) {
    const content = document.getElementById(`${sectionId}-content`);
    const toggle = document.getElementById(`${sectionId}-toggle`);
    
    if (content && toggle) {
        if (content.style.display === 'none') {
            // Expand the section
            content.style.display = 'block';
            toggle.textContent = '▼';
        } else {
            // Collapse the section
            content.style.display = 'none';
            toggle.textContent = '▶';
        }
    }
}

// Add zoom functionality for network view
function zoomIn() {
    if (network) {
        const currentScale = network.getScale();
        const newScale = Math.min(currentScale * 1.2, 5.0); // Max zoom of 5x
        network.moveTo({ scale: newScale });
    }
}

function zoomOut() {
    if (network) {
        const currentScale = network.getScale();
        const newScale = Math.max(currentScale / 1.2, 0.1); // Min zoom of 0.1x
        network.moveTo({ scale: newScale });
    }
}

function resetZoom() {
    if (network) {
        network.fit();
    }
}

// Add zoom buttons to the network container
function addZoomButtons() {
    const container = document.getElementById('networkContainer');
    if (!container) return;
    
    // Remove existing zoom buttons if they exist
    const existingZoomButtons = container.querySelector('.zoom-buttons');
    if (existingZoomButtons) {
        existingZoomButtons.remove();
    }
    
    // Create zoom buttons container
    const zoomButtonsContainer = document.createElement('div');
    zoomButtonsContainer.className = 'zoom-buttons';
    zoomButtonsContainer.innerHTML = `
        <button class="zoom-btn zoom-in-btn" onclick="zoomIn()" title="Zoom In (Ctrl + +)">
            <span>+</span>
        </button>
        <button class="zoom-btn zoom-out-btn" onclick="zoomOut()" title="Zoom Out (Ctrl + -)">
            <span>−</span>
        </button>
        <button class="zoom-btn zoom-reset-btn" onclick="resetZoom()" title="Reset Zoom (Ctrl + 0)">
            <span>⌂</span>
        </button>
    `;
    
    // Add zoom buttons to the network container
    container.appendChild(zoomButtonsContainer);
}

// Add keyboard shortcuts for zoom
function initializeZoomKeyboardShortcuts() {
    document.addEventListener('keydown', function(event) {
        // Only handle zoom shortcuts when network tab is active
        const networkTab = document.getElementById('networkTab');
        if (!networkTab || !networkTab.classList.contains('active')) {
            return;
        }
        
        // Check if Ctrl/Cmd is pressed
        if (event.ctrlKey || event.metaKey) {
            switch (event.key) {
                case '=':
                case '+':
                    event.preventDefault();
                    zoomIn();
                    break;
                case '-':
                    event.preventDefault();
                    zoomOut();
                    break;
                case '0':
                    event.preventDefault();
                    resetZoom();
                    break;
            }
        }
    });
}