import React, { useEffect, useRef, useCallback, useState } from 'react';
import { Network } from 'vis-network';
import { DataSet } from 'vis-data';
import { LineageData } from '../../types/LineageData';
import NetworkControls from './NetworkControls';
import NetworkModal from '../NetworkModal';
import NetworkStatistics from '../NetworkStatistics';
import './NetworkTab.css';

interface NetworkTabProps {
  data: LineageData;
  selectedNetworkScript: string | null;
  setSelectedNetworkScript: (script: string | null) => void;
  selectedTableFilters: string[];
  setSelectedTableFilters: (filters: string[]) => void;
}

interface NetworkNode {
  id: string;
  label: string;
  group: string;
  title: string;
  color: string;
  size: number;
  font?: {
    size: number;
    face: string;
  };
}

interface NetworkEdge {
  id: string;
  from: string;
  to: string;
  arrows: string;
  color: { color: string; opacity: number };
  width: number;
  title: string;
  label: string;
  font: {
    size: number;
    color: string;
  };
}

interface NetworkData {
  nodes: NetworkNode[];
  edges: NetworkEdge[];
}

const NetworkTab: React.FC<NetworkTabProps> = ({
  data,
  selectedNetworkScript,
  setSelectedNetworkScript,
  selectedTableFilters,
  setSelectedTableFilters
}) => {
  const [showStatistics, setShowStatistics] = useState(false);
  const [selectedEdge, setSelectedEdge] = useState<NetworkEdge | null>(null);
  const [showModal, setShowModal] = useState(false);
  const [lockViewEnabled, setLockViewEnabled] = useState(false);
  const [flowViewEnabled, setFlowViewEnabled] = useState(false);
  const [isFullscreen, setIsFullscreen] = useState(false);
  const [connectionMode, setConnectionMode] = useState<'direct' | 'impacts_by' | 'impacted_by' | 'both'>('direct');
  
  const networkContainerRef = useRef<HTMLDivElement>(null);
  const networkRef = useRef<Network | null>(null);

  // Function to format operations compactly (group by script name)
  const formatOperations = (operations: string[]): string => {
    const scriptGroups: { [scriptName: string]: number[] } = {};
    
    operations.forEach(op => {
      const match = op.match(/^(.*?)::op(\d+)$/);
      if (match) {
        const scriptName = match[1];
        const index = parseInt(match[2], 10);
        if (!scriptGroups[scriptName]) {
          scriptGroups[scriptName] = [];
        }
        scriptGroups[scriptName].push(index);
      }
    });
    
    return Object.entries(scriptGroups)
      .map(([scriptName, indices]) => `${scriptName}:${indices.sort((a, b) => a - b).join('|')}`)
      .join(', ');
  };

  // Build global ownership model (like old implementation)
  const buildOwnershipModel = () => {
    const allNodes: { [key: string]: any } = {};
    const allEdges: any[] = [];
    const edgeIds = new Set<string>(); // Track edge IDs to prevent duplicates
    
    console.log('=== buildOwnershipModel DEBUG ===');
    console.log('Data scripts:', data.scripts);
    
    if (!data.scripts) {
      console.log('No scripts data available');
      return { allNodes, allEdges };
    }
    
    // PASS 1: Build all nodes with proper ownership
    Object.entries(data.scripts).forEach(([scriptName, scriptData]: [string, any]) => {
      console.log(`Processing script: ${scriptName}`, scriptData);
      Object.entries(scriptData.tables || {}).forEach(([tableName, tableObj]: [string, any]) => {
        console.log(`  Processing table: ${tableName}`, tableObj);
        // Table names are already uppercase and global
        // Determine node ID based on volatility
        let nodeId: string;
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
          // Only merge properties for non-volatile tables (volatile tables are script-specific)
          if (!tableObj.is_volatile) {
            // If any definition has is_view: true, treat the table as a view
            if (tableObj.is_view) {
              allNodes[nodeId].is_view = true;
            }
          }
        } else {
          // Create new node
          allNodes[nodeId] = {
            id: nodeId,
            name: tableName,
            is_volatile: tableObj.is_volatile,
            is_view: tableObj.is_view,
            owners: [scriptName],
            source: tableObj.source || [],
            target: tableObj.target || [],
            properties: { ...tableObj, script_name: scriptName }
          };
        }
      });
    });
    
    // PASS 2: Create edges from relationships (consolidate operations from multiple scripts)
    console.log('=== PASS 2: Creating edges ===');
    const edgeMap = new Map<string, [string, string, string[]]>();
    
    Object.entries(data.scripts).forEach(([scriptName, scriptData]: [string, any]) => {
      console.log(`PASS 2 - Processing script: ${scriptName}`);
      Object.entries(scriptData.tables || {}).forEach(([tableName, tableObj]: [string, any]) => {
        console.log(`PASS 2 - Processing table: ${tableName}`, tableObj);
        // Convert table name to uppercase
        const upperTableName = tableName.toUpperCase();
        // Determine current node ID
        let currentNodeId: string;
        if (tableObj.is_volatile) {
          currentNodeId = `${scriptName}::${upperTableName}`;
        } else {
          currentNodeId = upperTableName;
        }
        
        // Process source relationships
        if (tableObj.source) {
          console.log(`Processing source relationships for ${upperTableName}:`, tableObj.source);
          tableObj.source.forEach((rel: any) => {
            console.log(`  Processing source relationship:`, rel);
            // Convert relationship table name to uppercase
            // Handle both formats: {table: 'name'} and {name: 'name'}
            const tableName = rel.table || rel.name;
            if (!rel || !tableName) {
              console.warn(`  Skipping source relationship - missing table/name:`, rel);
              return;
            }
            const upperRelName = tableName.toUpperCase();
            // Find source table - prioritize same script first
            let sourceTable = null;
            let sourceScript = null;
            
            // First, try to find the table in the current script
            if (scriptData.tables && scriptData.tables[tableName]) {
              sourceTable = scriptData.tables[tableName];
              sourceScript = scriptName;
            } else {
              // If not found in current script, look in other scripts
              for (const [sName, sData] of Object.entries(data.scripts || {})) {
                if (sData.tables && sData.tables[tableName]) {
                  sourceTable = sData.tables[tableName];
                  sourceScript = sName;
                  break;
                }
              }
            }
            
            // Determine source node ID
            let sourceNodeId: string;
            if (sourceTable && sourceTable.is_volatile) {
              sourceNodeId = `${sourceScript}::${upperRelName}`;
            } else {
              sourceNodeId = upperRelName;
            }
            
            // Create node for referenced table if it doesn't exist
            if (!allNodes[sourceNodeId]) {
              allNodes[sourceNodeId] = {
                id: sourceNodeId,
                name: upperRelName,
                is_volatile: sourceTable ? sourceTable.is_volatile : false,
                owners: sourceTable ? [sourceScript] : [],
                source: sourceTable ? (sourceTable.source || []) : [],
                target: sourceTable ? (sourceTable.target || []) : [],
                properties: sourceTable ? { ...sourceTable, script_name: sourceScript } : { name: upperRelName }
              };
            }
            
            // Create or update edge if both nodes exist and there are operations
            console.log(`  Checking edge creation: sourceNodeId=${sourceNodeId}, currentNodeId=${currentNodeId}`);
            console.log(`  Source node exists: ${!!allNodes[sourceNodeId]}, Current node exists: ${!!allNodes[currentNodeId]}`);
            console.log(`  Operations: ${rel.operation}, Length: ${rel.operation ? rel.operation.length : 0}`);
            
            if (allNodes[sourceNodeId] && allNodes[currentNodeId] && rel.operation && rel.operation.length > 0) {
              const edgeKey = `${sourceNodeId}->${currentNodeId}`;
              const existingEdge = edgeMap.get(edgeKey);
              
              if (existingEdge) {
                // Edge already exists, add operations from this script
                const existingOps = new Set(existingEdge[2]);
                rel.operation.forEach((opIndex: number) => {
                  existingOps.add(`${scriptName}::op${opIndex}`);
                });
                existingEdge[2] = Array.from(existingOps);
                console.log(`Updated existing edge: ${sourceNodeId} -> ${currentNodeId} with operations from ${scriptName}`);
              } else {
                // Create new edge
                const operations = rel.operation.map((opIndex: number) => `${scriptName}::op${opIndex}`);
                const newEdge: [string, string, string[]] = [sourceNodeId, currentNodeId, operations];
                allEdges.push(newEdge);
                edgeMap.set(edgeKey, newEdge);
                console.log(`✅ Created source edge: ${sourceNodeId} -> ${currentNodeId} with ${operations.length} operations from ${scriptName}`);
              }
            } else {
              console.log(`❌ Skipped edge creation - missing requirements`);
            }
          });
        }
        
        // Process target relationships
        if (tableObj.target) {
          console.log(`Processing target relationships for ${upperTableName}:`, tableObj.target);
          tableObj.target.forEach((rel: any) => {
            console.log(`  Processing target relationship:`, rel);
            // Convert relationship table name to uppercase
            // Handle both formats: {table: 'name'} and {name: 'name'}
            const tableName = rel.table || rel.name;
            if (!rel || !tableName) {
              console.warn(`  Skipping target relationship - missing table/name:`, rel);
              return;
            }
            const upperRelName = tableName.toUpperCase();
            // Find target table - prioritize same script first
            let targetTable = null;
            let targetScript = null;
            
            // First, try to find the table in the current script
            if (scriptData.tables && scriptData.tables[tableName]) {
              targetTable = scriptData.tables[tableName];
              targetScript = scriptName;
            } else {
              // If not found in current script, look in other scripts
              for (const [sName, sData] of Object.entries(data.scripts || {})) {
                if (sData.tables && sData.tables[tableName]) {
                  targetTable = sData.tables[tableName];
                  targetScript = sName;
                  break;
                }
              }
            }
            
            // Determine target node ID
            let targetNodeId: string;
            if (targetTable && targetTable.is_volatile) {
              targetNodeId = `${targetScript}::${upperRelName}`;
            } else {
              targetNodeId = upperRelName;
            }
            
            // Create node for referenced table if it doesn't exist
            if (!allNodes[targetNodeId]) {
              allNodes[targetNodeId] = {
                id: targetNodeId,
                name: upperRelName,
                is_volatile: targetTable ? targetTable.is_volatile : false,
                owners: targetTable ? [targetScript] : [],
                source: targetTable ? (targetTable.source || []) : [],
                target: targetTable ? (targetTable.target || []) : [],
                properties: targetTable ? { ...targetTable, script_name: targetScript } : { name: upperRelName }
              };
            }
            
            // Create or update edge if both nodes exist and there are operations
            if (allNodes[currentNodeId] && allNodes[targetNodeId] && rel.operation && rel.operation.length > 0) {
              const edgeKey = `${currentNodeId}->${targetNodeId}`;
              const existingEdge = edgeMap.get(edgeKey);
              
              if (existingEdge) {
                // Edge already exists, add operations from this script
                const existingOps = new Set(existingEdge[2]);
                rel.operation.forEach((opIndex: number) => {
                  existingOps.add(`${scriptName}::op${opIndex}`);
                });
                existingEdge[2] = Array.from(existingOps);
                console.log(`Updated existing edge: ${currentNodeId} -> ${targetNodeId} with operations from ${scriptName}`);
              } else {
                // Create new edge
                const operations = rel.operation.map((opIndex: number) => `${scriptName}::op${opIndex}`);
                const newEdge: [string, string, string[]] = [currentNodeId, targetNodeId, operations];
                allEdges.push(newEdge);
                edgeMap.set(edgeKey, newEdge);
                console.log(`✅ Created target edge: ${currentNodeId} -> ${targetNodeId} with ${operations.length} operations from ${scriptName}`);
              }
            }
          });
        }
      });
    });
    
    console.log(`Built ownership model: ${Object.keys(allNodes).length} nodes, ${allEdges.length} edges`);
    console.log('All nodes:', Object.keys(allNodes));
    console.log('All edges:', allEdges);
    console.log('=== END buildOwnershipModel DEBUG ===');
    return { allNodes, allEdges };
  };

  // Apply filters to the ownership model (from old implementation)
  const applyFilters = useCallback((scriptFilters: string[] = [], tableFilters: string[] = [], mode: string = 'direct') => {
    const { allNodes, allEdges } = buildOwnershipModel();
    let filteredNodes = Object.entries(allNodes);
    let relatedNodeIds: Set<string> | null = null;
    
    // 1. Apply script filters first
    if (scriptFilters.length > 0) {
      filteredNodes = filteredNodes.filter(([nodeId, node]) => {
        const hasMatchingOwner = node.owners.some((owner: string) => 
          scriptFilters.includes(owner)
        );
        return hasMatchingOwner;
      });
    }
    
    // 2. Apply table filters - show tables that match the filter AND their connected tables
    if (tableFilters.length > 0) {
      // First, find all nodes that match the table filter (search in ALL nodes, not just filtered ones)
      const matchingNodeIds = new Set<string>();
      relatedNodeIds = new Set<string>();
      
      // Add nodes that match the table filter from ALL nodes
      Object.entries(allNodes).forEach(([nodeId, node]) => {
        if (tableFilters.includes(node.name)) {
          matchingNodeIds.add(nodeId);
          relatedNodeIds!.add(nodeId);
        }
      });
      
      if (mode === 'impacts_by' || mode === 'impacted_by' || mode === 'both') {
        // Step 1: Find all nodes that the selected nodes can reach (downstream)
        const downstreamNodes = new Set<string>();
        const downstreamEdges = new Set<string>();
        
        if (mode === 'impacts_by' || mode === 'both') {
          const visited = new Set<string>();
          const queue = Array.from(matchingNodeIds);
          
          while (queue.length > 0) {
            const currentNodeId = queue.shift()!;
            
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
        const upstreamNodes = new Set<string>();
        const upstreamEdges = new Set<string>();
        
        if (mode === 'impacted_by' || mode === 'both') {
          const visited = new Set<string>();
          const queue = Array.from(matchingNodeIds);
          
          while (queue.length > 0) {
            const currentNodeId = queue.shift()!;
            
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
          downstreamNodes.forEach(nodeId => relatedNodeIds!.add(nodeId));
        } else if (mode === 'impacted_by') {
          upstreamNodes.forEach(nodeId => relatedNodeIds!.add(nodeId));
        } else if (mode === 'both') {
          downstreamNodes.forEach(nodeId => relatedNodeIds!.add(nodeId));
          upstreamNodes.forEach(nodeId => relatedNodeIds!.add(nodeId));
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
            relatedNodeIds!.add(to);
          }
          if (matchingNodeIds.has(to)) {
            // If the target node matches, add the source node
            relatedNodeIds!.add(from);
          }
        });
      }
      
      // Now filter nodes to only include the matching and related nodes
      // But also respect script filters if they were applied
      filteredNodes = Object.entries(allNodes).filter(([nodeId, node]) => {
        // Must be in the related set
        if (!relatedNodeIds!.has(nodeId)) {
          return false;
        }
        
        // If script filters are applied, must also match script filter
        if (scriptFilters.length > 0) {
          const hasMatchingOwner = node.owners.some((owner: string) => 
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
          const filteredOperations = operations.filter((op: string) => {
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
          return false;
        }
        return true;
      });
    
    return { 
      nodes: filteredNodes.map(([id, node]) => ({ id, ...node })), 
      edges: filteredEdges 
    };
  }, [buildOwnershipModel]);

  // Get available scripts and tables for search
  const availableScripts = React.useMemo(() => {
    if (!data || !data.scripts) return [];
    return Object.keys(data.scripts);
  }, [data]);

  const availableTables = React.useMemo(() => {
    if (!data || !data.scripts) return [];
    const tableNames = new Set<string>();
    Object.values(data.scripts).forEach((script: any) => {
      Object.keys(script.tables || {}).forEach(tableName => {
        tableNames.add(tableName.toUpperCase());
      });
    });
    return Array.from(tableNames).sort();
  }, [data]);

  // Search callback functions
  const handleScriptSearch = useCallback((scriptName: string) => {
    // If empty string, clear script selection
    if (scriptName === '') {
      setSelectedNetworkScript(null);
    } else {
      setSelectedNetworkScript(scriptName);
    }
  }, [setSelectedNetworkScript]);

  const handleTableSearch = useCallback((tableName: string) => {
    // If empty string, clear all table filters
    if (tableName === '') {
      setSelectedTableFilters([]);
    } else {
      // Only keep one table at a time - replace any existing table filter
      setSelectedTableFilters([tableName]);
    }
  }, [setSelectedTableFilters]);

  // Memoize buildOwnershipModel to avoid dependency issues
  const buildOwnershipModelMemo = React.useCallback(buildOwnershipModel, [data]);

  // Generate network data based on current filters using global ownership model
  const generateNetworkData = useCallback((): NetworkData => {
    console.log('Generating network data with filters:', { selectedNetworkScript, selectedTableFilters, connectionMode });
    console.log('Data structure:', data);
    console.log('Scripts available:', data?.scripts);
    
    if (!data || !data.scripts) {
      console.log('No scripts data available for network generation');
      return { nodes: [], edges: [] };
    }

    // Apply filters using the new applyFilters function
    const scriptFilters = selectedNetworkScript ? [selectedNetworkScript] : [];
    const { nodes: filteredNodes, edges: filteredEdges } = applyFilters(scriptFilters, selectedTableFilters, connectionMode);
    
    // Convert to vis.js format
    const nodes = filteredNodes.map(node => {
      // Calculate edge counts for this node
      const outgoingEdgeCount = filteredEdges.filter(([from]) => from === node.id).length;
      const incomingEdgeCount = filteredEdges.filter(([, to]) => to === node.id).length;
      
      // Determine node color based on edge counts, volatility, and view status
      let nodeColor = '#007bff'; // Default blue
      if (node.is_volatile) {
        nodeColor = '#ff9800'; // Orange for volatile tables
      } else if (node.is_view) {
        nodeColor = '#9c27b0'; // Purple for views
      } else if (incomingEdgeCount === 0) {
        nodeColor = '#28a745'; // Green for tables with no sources
      } else if (outgoingEdgeCount === 0) {
        nodeColor = '#dc3545'; // Red for tables with no targets
      }
      
      // Calculate node size based on edge count
      const nodeSize = 20 + Math.min(incomingEdgeCount + outgoingEdgeCount, 10) * 2;
      
      return {
        id: node.id,
        label: node.name,
        group: node.is_volatile ? 'volatile' : (node.is_view ? 'view' : 'normal'),
        title: `Table: ${node.name}\nScript: ${node.properties.script_name || 'unknown'}\nOwners: ${node.owners.join(', ')}\nVolatile: ${node.is_volatile ? 'Yes' : 'No'}\nView: ${node.is_view ? 'Yes' : 'No'}`,
        color: nodeColor,
        size: nodeSize
      };
    });
    
    const edges = filteredEdges.map(([from, to, operations]) => {
      // Format operations for display
      const operationTexts = formatOperations(operations);
      
      return {
        id: `${from}-${to}`,
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

    console.log(`Created ${nodes.length} nodes and ${edges.length} edges`);
    return { nodes, edges };
  }, [data, selectedNetworkScript, selectedTableFilters, connectionMode, applyFilters]);

  // Calculate optimized levels for better horizontal layout (from old implementation)
  const calculateOptimizedLevels = (nodes: NetworkNode[], edges: NetworkEdge[]) => {
    const optimizedLevels: { [key: string]: number } = {};
    
    // Calculate edge counts for all nodes
    const outgoingEdgeCounts: { [key: string]: number } = {};
    const incomingEdgeCounts: { [key: string]: number } = {};
    
    nodes.forEach(node => {
      outgoingEdgeCounts[node.id] = edges.filter(edge => edge.from === node.id).length;
      incomingEdgeCounts[node.id] = edges.filter(edge => edge.to === node.id).length;
    });
    
    // Find source nodes (nodes with no incoming edges)
    const sourceNodes = nodes.filter(node => incomingEdgeCounts[node.id] === 0);
    
    // Initialize levels for source nodes
    sourceNodes.forEach(node => {
      optimizedLevels[node.id] = 0;
    });
    
    // Use topological sort with optimization for horizontal layout
    const visited = new Set<string>();
    const queue = [...sourceNodes];
    
    while (queue.length > 0) {
      const currentNode = queue.shift()!;
      
      if (visited.has(currentNode.id)) {
        continue;
      }
      
      visited.add(currentNode.id);
      
      // Find children
      const children = edges
        .filter(edge => edge.from === currentNode.id)
        .map(edge => nodes.find(n => n.id === edge.to))
        .filter(Boolean) as NetworkNode[];
      
      children.forEach(child => {
        if (!visited.has(child.id)) {
          optimizedLevels[child.id] = (optimizedLevels[currentNode.id] || 0) + 1;
          queue.push(child);
        }
      });
    }
    
    return optimizedLevels;
  };

  // Calculate hierarchical levels with color-based separation for flow view (from old implementation)
  const calculateHierarchicalLevelsWithColorSeparation = (nodes: NetworkNode[], edges: NetworkEdge[]) => {
    const hierarchicalLevels: { [key: string]: { x: number; y: number; level: number; column: number; color: string } } = {};
    
    // Calculate optimized levels based on actual data flow
    const optimizedLevels = calculateOptimizedLevels(nodes, edges);
    
    // Group nodes by optimized level
    const levelGroups: { [key: number]: NetworkNode[] } = {};
    nodes.forEach(node => {
      const level = optimizedLevels[node.id] || 0;
      if (!levelGroups[level]) {
        levelGroups[level] = [];
      }
      levelGroups[level].push(node);
    });
    
    // Calculate positions for each level with color separation
    const levelSpacing = 200; // Horizontal spacing between levels
    const nodeSpacing = 150; // Vertical spacing between nodes
    const columnWidth = 150; // Width between columns
    
    Object.keys(levelGroups).forEach(levelStr => {
      const level = parseInt(levelStr);
      const nodesInLevel = levelGroups[level];
      
      // Separate nodes by color based on their role in the data flow
      const outgoingEdgeCounts: { [key: string]: number } = {};
      const incomingEdgeCounts: { [key: string]: number } = {};
      
      // Calculate edge counts for each node in this level
      nodesInLevel.forEach(node => {
        outgoingEdgeCounts[node.id] = edges.filter(edge => edge.from === node.id).length;
        incomingEdgeCounts[node.id] = edges.filter(edge => edge.to === node.id).length;
      });
      
      // Categorize nodes by color
      const greenNodes = nodesInLevel.filter(node => incomingEdgeCounts[node.id] === 0);
      const redNodes = nodesInLevel.filter(node => outgoingEdgeCounts[node.id] === 0);
      const orangeNodes = nodesInLevel.filter(node => node.group === 'volatile');
      const blueNodes = nodesInLevel.filter(node => 
        node.group !== 'volatile' && 
        incomingEdgeCounts[node.id] > 0 && 
        outgoingEdgeCounts[node.id] > 0
      );
      
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
          column: 0,
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
      
      // Place red nodes (no targets) - Target tables
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
        
        // Recalculate positions for green nodes if they need multiple columns
        if (greenColumns > 1) {
          greenNodes.forEach((node, index) => {
            const column = Math.floor(index / 5);
            const columnIndex = index % 5;
            
            const nodeX = x + column * (columnWidth / greenColumns);
            const totalHeight = (5 - 1) * nodeSpacing;
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
        
        // Recalculate positions for orange nodes if they need multiple columns
        if (orangeColumns > 1) {
          orangeNodes.forEach((node, index) => {
            const column = Math.floor(index / 5);
            const columnIndex = index % 5;
            
            const nodeX = x + columnWidth + column * (columnWidth / orangeColumns);
            const totalHeight = (5 - 1) * nodeSpacing;
            const startY = -totalHeight / 2;
            const y = startY + columnIndex * nodeSpacing;
            
            hierarchicalLevels[node.id] = {
              x: nodeX,
              y: y,
              level: level,
              column: 1 + column,
              color: 'orange'
            };
          });
        }
        
        // Recalculate positions for blue nodes if they need multiple columns
        if (blueColumns > 1) {
          blueNodes.forEach((node, index) => {
            const column = Math.floor(index / 5);
            const columnIndex = index % 5;
            
            const nodeX = x + (2 * columnWidth) + column * (columnWidth / blueColumns);
            const totalHeight = (5 - 1) * nodeSpacing;
            const startY = -totalHeight / 2;
            const y = startY + columnIndex * nodeSpacing;
            
            hierarchicalLevels[node.id] = {
              x: nodeX,
              y: y,
              level: level,
              column: 2 + column,
              color: 'blue'
            };
          });
        }
      }
    });
    
    return hierarchicalLevels;
  };

  // Apply flow view layout (hierarchical positioning)
  const applyFlowViewLayout = useCallback((nodes: NetworkNode[], edges: NetworkEdge[]) => {
    // Calculate hierarchical levels with color-based separation
    const hierarchicalLevels = calculateHierarchicalLevelsWithColorSeparation(nodes, edges);
    
    // Apply hierarchical positioning to nodes with color separation
    return nodes.map(node => {
      const level = hierarchicalLevels[node.id];
      if (level !== undefined) {
        return {
          ...node,
          x: level.x,
          y: level.y
        };
      }
      return node;
    });
  }, [calculateHierarchicalLevelsWithColorSeparation]);

  // Zoom functions
  const zoomIn = useCallback(() => {
    if (networkRef.current) {
      const currentScale = networkRef.current.getScale();
      const newScale = Math.min(currentScale * 1.2, 5.0); // Max zoom of 5x
      networkRef.current.moveTo({ scale: newScale });
    }
  }, []);

  const zoomOut = useCallback(() => {
    if (networkRef.current) {
      const currentScale = networkRef.current.getScale();
      const newScale = Math.max(currentScale / 1.2, 0.1); // Min zoom of 0.1x
      networkRef.current.moveTo({ scale: newScale });
    }
  }, []);

  const resetZoom = useCallback(() => {
    if (networkRef.current) {
      networkRef.current.fit();
    }
  }, []);

  const toggleFullscreen = useCallback(() => {
    setIsFullscreen(!isFullscreen);
  }, [isFullscreen]);

  const clearAllFilters = useCallback(() => {
    setSelectedNetworkScript(null);
    setSelectedTableFilters([]);
  }, [setSelectedNetworkScript, setSelectedTableFilters]);

  // Keyboard shortcuts for zoom
  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
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
    };

    document.addEventListener('keydown', handleKeyDown);
    return () => document.removeEventListener('keydown', handleKeyDown);
  }, [zoomIn, zoomOut, resetZoom]);

  // Initialize network visualization
  useEffect(() => {
    if (!networkContainerRef.current || !data) return;

    const { nodes, edges } = generateNetworkData();
    
    if (nodes.length === 0) {
      console.log('No nodes to display');
      return;
    }

    // Apply flow view layout if enabled
    const layoutNodes = flowViewEnabled ? applyFlowViewLayout(nodes, edges) : nodes;

    const nodesDataset = new DataSet(layoutNodes);
    const edgesDataset = new DataSet(edges);

    const networkData = {
      nodes: nodesDataset,
      edges: edgesDataset
    };

    const options = {
      nodes: {
        shape: 'box',
        borderWidth: 2,
        shadow: true,
        font: {
          size: 12,
          face: 'Arial'
        }
      },
      edges: {
        smooth: {
          enabled: true,
          type: 'curvedCW',
          roundness: 0.2
        }
      },
      physics: {
        enabled: false,
        stabilization: {
          enabled: false
        }
      },
      interaction: {
        hover: true,
        tooltipDelay: 0,
        zoomView: true,
        dragView: true,
        dragNodes: true,
        selectable: true,
        selectConnectedEdges: true
      }
    };

    // Destroy existing network
    if (networkRef.current) {
      networkRef.current.destroy();
    }

    // Create new network
    networkRef.current = new Network(networkContainerRef.current, networkData, options);

    // Handle click events
    networkRef.current.on('click', (params) => {
      if (params.nodes.length > 0) {
        // Node clicked - apply filter to show this table and its relationships
        const clickedNodeId = params.nodes[0];
        
        // Only apply filter if lock view is disabled
        if (!lockViewEnabled) {
          // Extract table name from node ID (remove script prefix for volatile tables)
          const tableName = clickedNodeId.includes('::') 
            ? clickedNodeId.split('::')[1] 
            : clickedNodeId;
          
          // Replace table filter with the clicked table (only keep one table at a time)
          setSelectedTableFilters([tableName]);
        } else {
          console.log('View is locked - node click ignored');
        }
      } else if (params.edges.length > 0) {
        // Edge clicked - show edge details
        const edgeId = params.edges[0];
        const edge = edges.find(e => e.id === edgeId);
        if (edge) {
          setSelectedEdge(edge);
          setShowModal(true);
        }
      }
    });

  }, [data, selectedNetworkScript, selectedTableFilters, generateNetworkData, flowViewEnabled, lockViewEnabled, applyFlowViewLayout, setSelectedTableFilters]);

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      if (networkRef.current) {
        networkRef.current.destroy();
      }
    };
  }, []);

  return (
    <div className={`network-tab ${isFullscreen ? 'fullscreen' : ''}`}>
      <div className="network-controls">
        <NetworkControls
          selectedScript={selectedNetworkScript}
          onScriptChange={setSelectedNetworkScript}
          onScriptSearch={handleScriptSearch}
          connectionMode={connectionMode}
          onConnectionModeChange={setConnectionMode}
          lockViewEnabled={lockViewEnabled}
          onLockViewToggle={setLockViewEnabled}
          flowViewEnabled={flowViewEnabled}
          onFlowViewToggle={setFlowViewEnabled}
          onShowStatistics={() => setShowStatistics(!showStatistics)}
          availableScripts={availableScripts}
          availableTables={availableTables}
          onTableSearch={handleTableSearch}
          selectedTableFilters={selectedTableFilters}
          onClearAll={clearAllFilters}
        />
      </div>
      
      <div className="network-container-wrapper">
        <div className="network-container">
          <div 
            ref={networkContainerRef} 
            id="networkContainer"
            style={{ width: '100%', height: isFullscreen ? 'calc(100vh - 200px)' : '100%', border: '1px solid #ccc' }}
          />
        </div>
        
        {/* Zoom buttons */}
        <div className="zoom-buttons">
          <button 
            className="zoom-btn zoom-in-btn" 
            onClick={zoomIn} 
            title="Zoom In (Ctrl + +)"
          >
            <span>+</span>
          </button>
          <button 
            className="zoom-btn zoom-out-btn" 
            onClick={zoomOut} 
            title="Zoom Out (Ctrl + -)"
          >
            <span>−</span>
          </button>
          <button 
            className="zoom-btn zoom-reset-btn" 
            onClick={resetZoom} 
            title="Reset Zoom (Ctrl + 0)"
          >
            <span>⌂</span>
          </button>
        </div>
        
        {/* Fullscreen toggle button */}
        <button 
          className="fullscreen-toggle-btn" 
          onClick={toggleFullscreen}
          title={isFullscreen ? 'Exit fullscreen mode' : 'Toggle fullscreen mode'}
        >
          {isFullscreen ? '❌' : '🔍'}
        </button>
      </div>
      
      {/* NetworkModal will be rendered when an edge is clicked */}
      {showModal && selectedEdge && (
        <NetworkModal 
          edge={selectedEdge}
          data={data} 
          onClose={() => {
            setShowModal(false);
            setSelectedEdge(null);
          }} 
        />
      )}
      {showStatistics && (
        <NetworkStatistics 
          data={data} 
          onClose={() => setShowStatistics(false)}
          selectedNetworkScript={selectedNetworkScript}
          selectedTableFilters={selectedTableFilters}
          connectionMode={connectionMode}
        />
      )}
    </div>
  );
};

export default NetworkTab;