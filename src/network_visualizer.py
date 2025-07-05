#!/usr/bin/env python3
"""
ETL Network Visualizer

This tool creates network visualizations of ETL lineage data using NetworkX and Plotly.
It shows tables as nodes and relationships as edges in a network graph format.

Usage:
    python network_visualizer.py <output_folder>
    python network_visualizer.py <json_file>
    
Example:
    python network_visualizer.py report_lm/
    python network_visualizer.py report_lm/CAMSTAR_LOT_BONUS_sh_lineage.json
"""

import json
import sys
import argparse
from pathlib import Path
from typing import Dict, List, Set, Tuple, Any
import networkx as nx
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import webbrowser
import os
import math


class NetworkVisualizer:
    """Creates network visualizations of ETL lineage data"""

    def __init__(self):
        self.node_colors = {
            'source': '#2E86AB',      # Blue for source tables
            'target': '#A23B72',      # Purple for target tables
            'volatile': '#F18F01',    # Orange for volatile tables
            'both': '#C73E1D'         # Red for tables that are both source and target
        }
        
        self.edge_colors = {
            'CREATE_VOLATILE': '#FF6B6B',
            'INSERT': '#4ECDC4',
            'UPDATE': '#45B7D1'
        }

    def load_lineage_data(self, file_path: str) -> Dict[str, Any]:
        """Load lineage data from JSON file"""
        with open(file_path, 'r') as f:
            return json.load(f)

    def build_network_graph(self, lineage_data: Dict[str, Any]) -> nx.DiGraph:
        """Build a NetworkX directed graph from lineage data"""
        G = nx.DiGraph()
        
        # Collect all unique tables
        all_tables = set()
        source_tables = set(lineage_data.get('source_tables', []))
        target_tables = set(lineage_data.get('target_tables', []))
        volatile_tables = set(lineage_data.get('volatile_tables', []))
        
        all_tables.update(source_tables)
        all_tables.update(target_tables)
        all_tables.update(volatile_tables)
        
        # Add nodes
        for table in all_tables:
            # Determine node type and color
            if table in volatile_tables:
                node_type = 'volatile'
                color = self.node_colors['volatile']
            elif table in source_tables and table in target_tables:
                node_type = 'both'
                color = self.node_colors['both']
            elif table in source_tables:
                node_type = 'source'
                color = self.node_colors['source']
            else:
                node_type = 'target'
                color = self.node_colors['target']
            
            G.add_node(table, type=node_type, color=color)
        
        # Add edges from operations
        for operation in lineage_data.get('operations', []):
            target_table = operation['target_table']
            source_tables = operation['source_tables']
            operation_type = operation['operation_type']
            
            for source_table in source_tables:
                if source_table in all_tables and target_table in all_tables:
                    # Add edge with operation details
                    G.add_edge(
                        source_table, 
                        target_table, 
                        operation_type=operation_type,
                        line_number=operation['line_number'],
                        color=self.edge_colors.get(operation_type, '#666666')
                    )
        
        return G

    def create_network_visualization(self, lineage_data: Dict[str, Any], output_file: str = None) -> go.Figure:
        """Create a network visualization using NetworkX and Plotly"""
        G = self.build_network_graph(lineage_data)
        
        if len(G.nodes()) == 0:
            print("No nodes found in the graph")
            return None
        
        # Use spring layout for positioning
        pos = nx.spring_layout(G, k=3, iterations=50, seed=42)
        
        # Extract node positions
        node_x = []
        node_y = []
        node_colors = []
        node_names = []
        node_types = []
        
        for node in G.nodes():
            x, y = pos[node]
            node_x.append(x)
            node_y.append(y)
            node_colors.append(G.nodes[node]['color'])
            node_names.append(node)
            node_types.append(G.nodes[node]['type'])
        
        # Create edge traces
        edge_x = []
        edge_y = []
        edge_colors = []
        edge_tooltips = []
        
        # Group edges by operation type
        edge_groups = {}
        for source, target, data in G.edges(data=True):
            op_type = data['operation_type']
            if op_type not in edge_groups:
                edge_groups[op_type] = []
            edge_groups[op_type].append((source, target, data))
        
        # Create the figure
        fig = go.Figure()
        
        # Add edge traces for each operation type
        for op_type, edges in edge_groups.items():
            edge_x = []
            edge_y = []
            edge_tooltips = []
            
            for source, target, data in edges:
                x0, y0 = pos[source]
                x1, y1 = pos[target]
                
                # Create arrow by shortening the line slightly
                # Calculate direction vector
                dx = x1 - x0
                dy = y1 - y0
                length = (dx**2 + dy**2)**0.5
                
                if length > 0:
                    # Normalize and scale back the end point to create arrow space
                    scale = 0.85  # Shorten line by 15%
                    x1_arrow = x0 + dx * scale
                    y1_arrow = y0 + dy * scale
                    
                    edge_x.extend([x0, x1_arrow, None])
                    edge_y.extend([y0, y1_arrow, None])
                    
                    # Create tooltip with operation details
                    tooltip = f"<b>{source} → {target}</b><br>"
                    tooltip += f"Operation: {op_type}<br>"
                    tooltip += f"Line: {data['line_number']}"
                    edge_tooltips.extend([tooltip, tooltip, None])
            
            edge_color = self.edge_colors.get(op_type, '#666666')
            fig.add_trace(go.Scatter(
                x=edge_x,
                y=edge_y,
                mode='lines',
                line=dict(width=4, color=edge_color),
                hoverinfo='text',
                text=edge_tooltips,
                hoverlabel=dict(
                    bgcolor="white",
                    bordercolor="black",
                    font_size=12,
                    font_family="Arial"
                ),
                showlegend=False,
                opacity=0.7,
                name=op_type
            ))
            
            # Add arrows for each edge
            for source, target, data in edges:
                x0, y0 = pos[source]
                x1, y1 = pos[target]
                
                # Calculate arrow position (at 85% of the line)
                dx = x1 - x0
                dy = y1 - y0
                length = (dx**2 + dy**2)**0.5
                
                if length > 0:
                    # Position arrow at 85% of the line
                    arrow_x = x0 + dx * 0.85
                    arrow_y = y0 + dy * 0.85
                    
                    # Calculate arrow direction
                    angle = math.atan2(dy, dx)
                    
                    # Create arrow head
                    arrow_size = 0.05
                    arrow_angle1 = angle + math.pi/6
                    arrow_angle2 = angle - math.pi/6
                    
                    arrow_x1 = arrow_x - arrow_size * math.cos(arrow_angle1)
                    arrow_y1 = arrow_y - arrow_size * math.sin(arrow_angle1)
                    arrow_x2 = arrow_x - arrow_size * math.cos(arrow_angle2)
                    arrow_y2 = arrow_y - arrow_size * math.sin(arrow_angle2)
                    
                    # Add arrow head
                    fig.add_trace(go.Scatter(
                        x=[arrow_x, arrow_x1, None, arrow_x, arrow_x2],
                        y=[arrow_y, arrow_y1, None, arrow_y, arrow_y2],
                        mode='lines',
                        line=dict(width=3, color=edge_color),
                        showlegend=False,
                        hoverinfo='skip'
                    ))
        
        # Create node tooltips
        node_tooltips = []
        node_types_for_hover = []
        for node in G.nodes():
            tooltip = f"<b>{node}</b><br>"
            tooltip += f"Type: {G.nodes[node]['type'].title()}<br>"
            
            # Count incoming and outgoing edges
            incoming = G.in_degree(node)
            outgoing = G.out_degree(node)
            
            tooltip += f"Incoming: {incoming}<br>"
            tooltip += f"Outgoing: {outgoing}"
            node_tooltips.append(tooltip)
            node_types_for_hover.append(G.nodes[node]['type'].title())
        
        # Add nodes
        fig.add_trace(go.Scatter(
            x=node_x,
            y=node_y,
            mode='markers+text',
            marker=dict(
                size=20,
                color=node_colors,
                line=dict(width=2, color='white')
            ),
            text=node_names,
            textposition="bottom center",
            hoverinfo='text',
            textfont=dict(size=10),
            hovertext=node_tooltips,
            showlegend=False,
            customdata=list(zip(node_names, node_types_for_hover)),  # Add custom data for selection
            hovertemplate='<b>%{customdata[0]}</b><br>Type: %{customdata[1]}<extra></extra>'
        ))
        
        # Update layout
        fig.update_layout(
            title=f"Network View: {lineage_data['script_name']}",
            title_x=0.5,
            showlegend=False,
            hovermode='closest',
            hoverdistance=100,
            spikedistance=1000,
            margin=dict(b=20, l=5, r=5, t=40),
            xaxis=dict(
                showgrid=False, 
                zeroline=False, 
                showticklabels=False,
                fixedrange=True  # Disable zoom and pan on x-axis
            ),
            yaxis=dict(
                showgrid=False, 
                zeroline=False, 
                showticklabels=False,
                fixedrange=True  # Disable zoom and pan on y-axis
            ),
            plot_bgcolor='white',
            width=None,  # Use responsive width
            height=None,  # Use responsive height
            autosize=True,  # Enable autosize
            # Disable modebar (zoom, pan, select tools)
            modebar=dict(remove=['zoom', 'pan', 'select', 'lasso', 'zoomIn', 'zoomOut', 'autoScale', 'resetScale'])
        )
        
        # Add instruction annotation
        fig.add_annotation(
            text="Click on a node to highlight its connections",
            xref="paper", yref="paper",
            x=0.5, y=1.02,
            showarrow=False,
            font=dict(size=12, color="gray"),
            bgcolor="rgba(255,255,255,0.8)",
            bordercolor="gray",
            borderwidth=1
        )
        
        # Add reset button
        fig.update_layout(
            updatemenus=[
                dict(
                    type="buttons",
                    direction="left",
                    pad=dict(r=10, t=87),
                    showactive=False,
                    x=0.1,
                    xanchor="right",
                    y=0,
                    yanchor="top",
                    buttons=[
                        dict(
                            label="Reset View",
                            method="restyle",
                            args=[{"selectedpoints": [None]}]
                        )
                    ]
                )
            ]
        )
        
        # Add legend
        legend_items = []
        for node_type, color in self.node_colors.items():
            legend_items.append(
                go.Scatter(
                    x=[None], y=[None],
                    mode='markers',
                    marker=dict(size=10, color=color),
                    name=f"{node_type.title()} Tables",
                    showlegend=True
                )
            )
        
        for operation_type, color in self.edge_colors.items():
            legend_items.append(
                go.Scatter(
                    x=[None], y=[None],
                    mode='lines',
                    line=dict(width=2, color=color),
                    name=f"{operation_type} Operations",
                    showlegend=True
                )
            )
        
        for item in legend_items:
            fig.add_trace(item)
        
        if output_file:
            # Create the HTML with custom JavaScript for node selection
            html_content = fig.to_html(include_plotlyjs=True, full_html=True)
            
            # Add custom CSS for responsive sizing
            custom_css = """
            <style>
            body {
                margin: 0;
                padding: 0;
                width: 100vw;
                height: 100vh;
                overflow: hidden;
            }
            
            .plotly-graph-div {
                width: 100% !important;
                height: 100% !important;
                position: absolute !important;
                top: 0 !important;
                left: 0 !important;
            }
            
            .js-plotly-plot {
                width: 100% !important;
                height: 100% !important;
            }
            
            .plot-container {
                width: 100% !important;
                height: 100% !important;
            }
            
            svg {
                width: 100% !important;
                height: 100% !important;
            }
            </style>
            """
            
            # Insert the custom CSS in the head section
            html_content = html_content.replace('</head>', custom_css + '</head>')
            
            # Add custom JavaScript for node selection highlighting
            custom_js = """
            <script>
            document.addEventListener('DOMContentLoaded', function() {
                var graphDiv = document.querySelector('.plotly-graph-div');
                
                // Make the plot responsive
                window.addEventListener('resize', function() {
                    Plotly.Plots.resize(graphDiv);
                });
                
                // Store original opacities
                var originalOpacities = [];
                var traces = graphDiv.data;
                for (var i = 0; i < traces.length; i++) {
                    originalOpacities.push(traces[i].opacity || 1.0);
                }
                
                // Track current selection
                var currentSelection = null;
                
                graphDiv.on('plotly_click', function(data) {
                    console.log('Click event:', data);
                    var point = data.points[0];
                    
                    // Only handle node clicks (not edge clicks)
                    if (point.mode && point.mode.includes('markers')) {
                        console.log('Node clicked:', point);
                        var nodeName = point.customdata ? point.customdata[0] : point.text;
                        console.log('Node name:', nodeName);
                        
                        // If clicking the same node, reset view
                        if (currentSelection === nodeName) {
                            console.log('Resetting view');
                            resetNetworkView();
                            currentSelection = null;
                            return;
                        }
                        
                        currentSelection = nodeName;
                        console.log('Current selection:', currentSelection);
                        
                        // Find connected nodes and edges
                        var connectedNodes = [nodeName];
                        var connectedEdges = [];
                        
                        // Find all edges connected to the selected node
                        for (var i = 0; i < traces.length; i++) {
                            if (traces[i].mode && traces[i].mode.includes('lines') && 
                                traces[i].text && traces[i].text.length > 0) {
                                
                                // Check each edge's tooltip for connections
                                for (var j = 0; j < traces[i].text.length; j++) {
                                    var tooltip = traces[i].text[j];
                                    if (tooltip && typeof tooltip === 'string') {
                                        // Extract source and target from tooltip
                                        if (tooltip.includes(' → ')) {
                                            var parts = tooltip.split(' → ');
                                            if (parts.length === 2) {
                                                var source = parts[0].replace('<b>', '').replace('</b>', '');
                                                var target = parts[1].split('<br>')[0].replace('</b>', '');
                                                
                                                if (source === nodeName) {
                                                    if (!connectedNodes.includes(target)) {
                                                        connectedNodes.push(target);
                                                    }
                                                    if (!connectedEdges.includes(i)) {
                                                        connectedEdges.push(i);
                                                    }
                                                } else if (target === nodeName) {
                                                    if (!connectedNodes.includes(source)) {
                                                        connectedNodes.push(source);
                                                    }
                                                    if (!connectedEdges.includes(i)) {
                                                        connectedEdges.push(i);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        
                        console.log('Connected nodes:', connectedNodes);
                        console.log('Connected edges:', connectedEdges);
                        
                        // Update opacities for all traces
                        var updates = [];
                        for (var i = 0; i < traces.length; i++) {
                            var newOpacity = 0.1; // Default to greyed out
                            
                            if (traces[i].mode && traces[i].mode.includes('markers')) {
                                // For nodes
                                var isConnected = false;
                                for (var j = 0; j < traces[i].x.length; j++) {
                                    var nodeText = traces[i].customdata ? traces[i].customdata[j][0] : traces[i].text[j];
                                    if (nodeText && connectedNodes.includes(nodeText)) {
                                        isConnected = true;
                                        break;
                                    }
                                }
                                newOpacity = isConnected ? 1.0 : 0.2;
                            } else if (traces[i].mode && traces[i].mode.includes('lines')) {
                                // For edges
                                var isConnected = connectedEdges.includes(i);
                                newOpacity = isConnected ? 1.0 : 0.1;
                            }
                            
                            updates.push({opacity: newOpacity});
                        }
                        
                        // Apply updates
                        Plotly.restyle(graphDiv, updates);
                    }
                });
                
                // Add reset functionality
                window.resetNetworkView = function() {
                    console.log('Resetting network view');
                    var updates = [];
                    for (var i = 0; i < traces.length; i++) {
                        updates.push({opacity: originalOpacities[i]});
                    }
                    Plotly.restyle(graphDiv, updates);
                    currentSelection = null;
                };
            });
            </script>
            """
            
            # Insert the custom JavaScript before the closing body tag
            html_content = html_content.replace('</body>', custom_js + '</body>')
            
            # Add a reset button with JavaScript function call
            reset_button_js = """
            <script>
            // Add reset button functionality
            document.addEventListener('DOMContentLoaded', function() {
                var resetBtn = document.querySelector('button[data-val="Reset View"]');
                if (resetBtn) {
                    resetBtn.onclick = function() {
                        if (typeof resetNetworkView === 'function') {
                            resetNetworkView();
                        }
                    };
                }
            });
            </script>
            """
            
            html_content = html_content.replace('</body>', reset_button_js + '</body>')
            
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(html_content)
            print(f"💾 Network visualization saved to: {output_file}")
        
        return fig

    def create_global_network(self, lineage_files: List[str], output_file: str = None) -> go.Figure:
        """Create a global network showing all tables and relationships across all scripts"""
        if not lineage_files:
            print("No lineage files found")
            return None
        
        # Build a global graph
        global_G = nx.DiGraph()
        
        # Load all lineage data
        all_data = []
        for file_path in lineage_files:
            try:
                data = self.load_lineage_data(file_path)
                data['file_path'] = file_path
                all_data.append(data)
            except Exception as e:
                print(f"Warning: Could not load {file_path}: {e}")
        
        if not all_data:
            print("No valid lineage data found")
            return None
        
        # Collect all tables and their types across all scripts
        all_tables = set()
        table_types = {}
        table_usage_count = {}
        
        for data in all_data:
            source_tables = set(data.get('source_tables', []))
            target_tables = set(data.get('target_tables', []))
            volatile_tables = set(data.get('volatile_tables', []))
            
            all_tables.update(source_tables)
            all_tables.update(target_tables)
            all_tables.update(volatile_tables)
            
            # Track table usage
            for table in source_tables | target_tables | volatile_tables:
                table_usage_count[table] = table_usage_count.get(table, 0) + 1
            
            # Determine table types (prioritize volatile > both > source/target)
            for table in volatile_tables:
                if table not in table_types or table_types[table] == 'source':
                    table_types[table] = 'volatile'
            
            for table in source_tables & target_tables:
                if table not in table_types or table_types[table] not in ['volatile']:
                    table_types[table] = 'both'
            
            for table in source_tables:
                if table not in table_types:
                    table_types[table] = 'source'
            
            for table in target_tables:
                if table not in table_types:
                    table_types[table] = 'target'
        
        # Add nodes to global graph
        for table in all_tables:
            node_type = table_types.get(table, 'source')
            color = self.node_colors[node_type]
            global_G.add_node(table, type=node_type, color=color, usage_count=table_usage_count.get(table, 0))
        
        # Add edges from all operations
        for data in all_data:
            for operation in data.get('operations', []):
                target_table = operation['target_table']
                source_tables = operation['source_tables']
                operation_type = operation['operation_type']
                
                for source_table in source_tables:
                    if source_table in all_tables and target_table in all_tables:
                        # Check if edge already exists
                        if global_G.has_edge(source_table, target_table):
                            # Update existing edge with operation info
                            edge_data = global_G[source_table][target_table]
                            if 'operations' not in edge_data:
                                edge_data['operations'] = []
                            edge_data['operations'].append({
                                'type': operation_type,
                                'script': data['script_name'],
                                'line': operation['line_number']
                            })
                        else:
                            # Add new edge
                            global_G.add_edge(
                                source_table, 
                                target_table, 
                                operations=[{
                                    'type': operation_type,
                                    'script': data['script_name'],
                                    'line': operation['line_number']
                                }],
                                color=self.edge_colors.get(operation_type, '#666666')
                            )
        
        if len(global_G.nodes()) == 0:
            print("No nodes found in the global graph")
            return None
        
        # Use spring layout for positioning
        pos = nx.spring_layout(global_G, k=2, iterations=100, seed=42)
        
        # Extract node positions and properties
        node_x = []
        node_y = []
        node_colors = []
        node_names = []
        node_sizes = []
        node_tooltips = []
        
        for node in global_G.nodes():
            x, y = pos[node]
            node_x.append(x)
            node_y.append(y)
            node_colors.append(global_G.nodes[node]['color'])
            node_names.append(node)
            
            # Size based on usage count
            usage_count = global_G.nodes[node]['usage_count']
            size = max(15, min(40, 15 + usage_count * 2))
            node_sizes.append(size)
            
            # Create tooltip
            tooltip = f"<b>{node}</b><br>"
            tooltip += f"Type: {global_G.nodes[node]['type'].title()}<br>"
            tooltip += f"Usage: {usage_count} scripts<br>"
            
            incoming = global_G.in_degree(node)
            outgoing = global_G.out_degree(node)
            tooltip += f"Incoming: {incoming}<br>"
            tooltip += f"Outgoing: {outgoing}"
            node_tooltips.append(tooltip)
        
        # Create the figure
        fig = go.Figure()
        
        # Add edges
        edge_x = []
        edge_y = []
        edge_tooltips = []
        
        for source, target, data in global_G.edges(data=True):
            x0, y0 = pos[source]
            x1, y1 = pos[target]
            
            # Create arrow by shortening the line slightly
            dx = x1 - x0
            dy = y1 - y0
            length = (dx**2 + dy**2)**0.5
            
            if length > 0:
                # Normalize and scale back the end point to create arrow space
                scale = 0.85  # Shorten line by 15%
                x1_arrow = x0 + dx * scale
                y1_arrow = y0 + dy * scale
                
                edge_x.extend([x0, x1_arrow, None])
                edge_y.extend([y0, y1_arrow, None])
                
                # Create edge tooltip
                operations = data.get('operations', [])
                tooltip = f"<b>{source} → {target}</b><br>"
                tooltip += f"Operations: {len(operations)}<br>"
                for op in operations[:3]:  # Show first 3 operations
                    tooltip += f"• {op['type']} ({op['script']})<br>"
                if len(operations) > 3:
                    tooltip += f"... and {len(operations) - 3} more"
                
                edge_tooltips.extend([tooltip, tooltip, None])
        
        # Add edge trace
        fig.add_trace(go.Scatter(
            x=edge_x,
            y=edge_y,
            mode='lines',
            line=dict(width=3, color='#cccccc'),
            hoverinfo='text',
            text=edge_tooltips,
            hoverlabel=dict(
                bgcolor="white",
                bordercolor="black",
                font_size=12,
                font_family="Arial"
            ),
            showlegend=False,
            opacity=0.5
        ))
        
        # Add arrows for each edge
        for source, target, data in global_G.edges(data=True):
            x0, y0 = pos[source]
            x1, y1 = pos[target]
            
            # Calculate arrow position (at 85% of the line)
            dx = x1 - x0
            dy = y1 - y0
            length = (dx**2 + dy**2)**0.5
            
            if length > 0:
                # Position arrow at 85% of the line
                arrow_x = x0 + dx * 0.85
                arrow_y = y0 + dy * 0.85
                
                # Calculate arrow direction
                angle = math.atan2(dy, dx)
                
                # Create arrow head
                arrow_size = 0.03  # Smaller arrows for global network
                arrow_angle1 = angle + math.pi/6
                arrow_angle2 = angle - math.pi/6
                
                arrow_x1 = arrow_x - arrow_size * math.cos(arrow_angle1)
                arrow_y1 = arrow_y - arrow_size * math.sin(arrow_angle1)
                arrow_x2 = arrow_x - arrow_size * math.cos(arrow_angle2)
                arrow_y2 = arrow_y - arrow_size * math.sin(arrow_angle2)
                
                # Add arrow head
                fig.add_trace(go.Scatter(
                    x=[arrow_x, arrow_x1, None, arrow_x, arrow_x2],
                    y=[arrow_y, arrow_y1, None, arrow_y, arrow_y2],
                    mode='lines',
                    line=dict(width=2, color='#cccccc'),
                    showlegend=False,
                    hoverinfo='skip'
                ))
        
        # Add nodes
        fig.add_trace(go.Scatter(
            x=node_x,
            y=node_y,
            mode='markers+text',
            marker=dict(
                size=node_sizes,
                color=node_colors,
                line=dict(width=2, color='white')
            ),
            text=node_names,
            textposition="bottom center",
            hoverinfo='text',
            textfont=dict(size=8),
            hovertext=node_tooltips,
            showlegend=False
        ))
        
        # Update layout
        fig.update_layout(
            title="Global ETL Network - All Tables and Relationships",
            title_x=0.5,
            showlegend=False,
            hovermode='closest',
            hoverdistance=100,
            spikedistance=1000,
            margin=dict(b=20, l=5, r=5, t=40),
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            plot_bgcolor='white',
            width=1200,
            height=800
        )
        
        # Add legend
        legend_items = []
        for node_type, color in self.node_colors.items():
            legend_items.append(
                go.Scatter(
                    x=[None], y=[None],
                    mode='markers',
                    marker=dict(size=10, color=color),
                    name=f"{node_type.title()} Tables",
                    showlegend=True
                )
            )
        
        for item in legend_items:
            fig.add_trace(item)
        
        if output_file:
            fig.write_html(output_file)
            print(f"💾 Global network visualization saved to: {output_file}")
        
        return fig

    def process_folder(self, folder_path: str) -> None:
        """Process all JSON files in a folder and create network visualizations"""
        folder = Path(folder_path)
        if not folder.exists():
            print(f"Folder not found: {folder_path}")
            return
        
        # Find all JSON files
        json_files = list(folder.glob("*_lineage.json"))
        
        if not json_files:
            print(f"No lineage JSON files found in {folder_path}")
            return
        
        print(f"Found {len(json_files)} lineage files")
        
        # Create individual network visualizations
        for json_file in json_files:
            try:
                print(f"Processing: {json_file.name}")
                lineage_data = self.load_lineage_data(str(json_file))
                
                # Create network visualization
                network_file = json_file.with_name(f"{json_file.stem}_network.html")
                self.create_network_visualization(lineage_data, str(network_file))
                
            except Exception as e:
                print(f"Error processing {json_file.name}: {e}")
        
        # Create global network visualization
        if len(json_files) > 1:
            global_network_file = folder / "global_network.html"
            self.create_global_network([str(f) for f in json_files], str(global_network_file))
        
        print(f"\n✅ Processed {len(json_files)} files")
        print(f"📁 Output folder: {folder}")

    def process_single_file(self, file_path: str, output_file: str = None) -> None:
        """Process a single JSON file and create network visualization"""
        try:
            lineage_data = self.load_lineage_data(file_path)
            
            if output_file is None:
                output_file = Path(file_path).with_name(f"{Path(file_path).stem}_network.html")
            
            fig = self.create_network_visualization(lineage_data, output_file)
            
            # Open in browser
            webbrowser.open(f'file://{os.path.abspath(output_file)}')
            
        except Exception as e:
            print(f"Error processing file: {e}")


def main():
    """Main function to run the network visualizer"""
    parser = argparse.ArgumentParser(
        description="Create network visualizations of ETL lineage data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process all JSON files in a folder
  python network_visualizer.py report_lm/
  
  # Process a single JSON file
  python network_visualizer.py report_lm/CAMSTAR_LOT_BONUS_sh_lineage.json
  
  # Specify output file
  python network_visualizer.py report_lm/CAMSTAR_LOT_BONUS_sh_lineage.json --output my_network.html
        """
    )
    
    parser.add_argument(
        "input",
        help="Input folder containing JSON files OR single JSON file path"
    )
    
    parser.add_argument(
        "--output",
        help="Output HTML file (for single file mode)"
    )
    
    args = parser.parse_args()
    
    try:
        visualizer = NetworkVisualizer()
        input_path = Path(args.input)
        
        if input_path.is_file():
            # Single file mode
            visualizer.process_single_file(args.input, args.output)
        elif input_path.is_dir():
            # Folder mode
            visualizer.process_folder(args.input)
        else:
            print(f"Input path does not exist: {args.input}")
            sys.exit(1)
            
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 