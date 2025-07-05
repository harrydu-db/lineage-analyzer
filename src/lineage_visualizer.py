#!/usr/bin/env python3
"""
ETL Lineage Visualizer

This tool reads JSON lineage files and creates an interactive web-based visualization
of data lineage relationships using Plotly.

Usage:
    python lineage_visualizer.py <output_folder>
    python lineage_visualizer.py <json_file>
    
Example:
    python lineage_visualizer.py reports/
    python lineage_visualizer.py reports/CAMSTAR_LOT_BONUS_sh_lineage.json
"""

import json
import sys
import argparse
from pathlib import Path
from typing import Dict, List, Set, Tuple, Any
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px
from datetime import datetime
import webbrowser
import os


class LineageVisualizer:
    """Creates interactive visualizations of ETL lineage data"""

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

    def extract_nodes_and_edges(self, lineage_data: Dict[str, Any]) -> Tuple[List[Dict], List[Dict]]:
        """Extract nodes and edges from lineage data"""
        nodes = []
        edges = []
        node_ids = set()
        
        # Collect all unique tables
        all_tables = set()
        source_tables = set(lineage_data.get('source_tables', []))
        target_tables = set(lineage_data.get('target_tables', []))
        volatile_tables = set(lineage_data.get('volatile_tables', []))
        
        all_tables.update(source_tables)
        all_tables.update(target_tables)
        all_tables.update(volatile_tables)
        
        # Create nodes
        for table in sorted(all_tables):
            node_id = len(nodes)
            node_ids.add(table)
            
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
            
            nodes.append({
                'id': node_id,
                'name': table,
                'type': node_type,
                'color': color,
                'x': 0,  # Will be calculated by layout
                'y': 0   # Will be calculated by layout
            })
        
        # Create edges from operations
        for operation in lineage_data.get('operations', []):
            target_table = operation['target_table']
            source_tables = operation['source_tables']
            operation_type = operation['operation_type']
            
            for source_table in source_tables:
                if source_table in node_ids and target_table in node_ids:
                    # Find node IDs
                    source_id = next(n['id'] for n in nodes if n['name'] == source_table)
                    target_id = next(n['id'] for n in nodes if n['name'] == target_table)
                    
                    # Create edge
                    edge_color = self.edge_colors.get(operation_type, '#666666')
                    tooltip = f"{operation_type}<br>Line: {operation['line_number']}"
                    
                    edges.append({
                        'source': source_id,
                        'target': target_id,
                        'operation_type': operation_type,
                        'color': edge_color,
                        'tooltip': tooltip,
                        'line_number': operation['line_number']
                    })
        
        return nodes, edges

    def create_hierarchical_layout(self, nodes: List[Dict], edges: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
        """Create a hierarchical layout for the nodes"""
        # Create adjacency lists
        in_edges = {i: [] for i in range(len(nodes))}
        out_edges = {i: [] for i in range(len(nodes))}
        
        for edge in edges:
            in_edges[edge['target']].append(edge['source'])
            out_edges[edge['source']].append(edge['target'])
        
        # Find root nodes (nodes with no incoming edges)
        root_nodes = [i for i in range(len(nodes)) if not in_edges[i]]
        
        # Find leaf nodes (nodes with no outgoing edges)
        leaf_nodes = [i for i in range(len(nodes)) if not out_edges[i]]
        
        # Calculate levels using BFS
        levels = {}
        visited = set()
        
        # Start from root nodes
        for root in root_nodes:
            if root not in visited:
                queue = [(root, 0)]
                while queue:
                    node, level = queue.pop(0)
                    if node not in visited:
                        visited.add(node)
                        levels[node] = level
                        
                        # Add children to queue
                        for child in out_edges[node]:
                            if child not in visited:
                                queue.append((child, level + 1))
        
        # Assign positions
        level_groups = {}
        for node_id, level in levels.items():
            if level not in level_groups:
                level_groups[level] = []
            level_groups[level].append(node_id)
        
        # Position nodes
        max_level = max(levels.values()) if levels else 0
        level_width = 800 / (max_level + 1) if max_level > 0 else 800
        
        for level, node_ids in level_groups.items():
            x = level * level_width + 100
            y_spacing = 600 / (len(node_ids) + 1)
            
            for i, node_id in enumerate(sorted(node_ids)):
                y = (i + 1) * y_spacing + 50
                nodes[node_id]['x'] = x
                nodes[node_id]['y'] = y
        
        # Position any unassigned nodes
        for i, node in enumerate(nodes):
            if 'x' not in node or node['x'] == 0:
                node['x'] = 400
                node['y'] = 300
        
        return nodes, edges

    def create_lineage_graph(self, lineage_data: Dict[str, Any], output_file: str = None) -> go.Figure:
        """Create an interactive lineage graph"""
        nodes, edges = self.extract_nodes_and_edges(lineage_data)
        nodes, edges = self.create_hierarchical_layout(nodes, edges)
        
        # Create the figure
        fig = go.Figure()
        
        # Group edges by operation type for separate traces
        edge_groups = {}
        for edge in edges:
            op_type = edge['operation_type']
            if op_type not in edge_groups:
                edge_groups[op_type] = []
            edge_groups[op_type].append(edge)
        
        # Add edge traces for each operation type
        for op_type, op_edges in edge_groups.items():
            edge_x = []
            edge_y = []
            edge_tooltips = []
            
            for edge in op_edges:
                source_node = next(n for n in nodes if n['id'] == edge['source'])
                target_node = next(n for n in nodes if n['id'] == edge['target'])
                
                # Add edge line
                edge_x.extend([source_node['x'], target_node['x'], None])
                edge_y.extend([source_node['y'], target_node['y'], None])
                edge_tooltips.extend([edge['tooltip'], edge['tooltip'], None])
            
            # Add edge trace for this operation type
            edge_color = self.edge_colors.get(op_type, '#666666')
            fig.add_trace(go.Scatter(
                x=edge_x,
                y=edge_y,
                mode='lines',
                line=dict(width=2, color=edge_color),
                hoverinfo='text',
                text=edge_tooltips,
                showlegend=False,
                opacity=0.7,
                name=op_type
            ))
        
        # Add nodes
        node_x = [node['x'] for node in nodes]
        node_y = [node['y'] for node in nodes]
        node_colors = [node['color'] for node in nodes]
        node_names = [node['name'] for node in nodes]
        node_types = [node['type'] for node in nodes]
        
        # Create node tooltips
        node_tooltips = []
        for node in nodes:
            tooltip = f"<b>{node['name']}</b><br>"
            tooltip += f"Type: {node['type'].title()}<br>"
            
            # Count incoming and outgoing edges
            incoming = len([e for e in edges if e['target'] == node['id']])
            outgoing = len([e for e in edges if e['source'] == node['id']])
            
            tooltip += f"Incoming: {incoming}<br>"
            tooltip += f"Outgoing: {outgoing}"
            node_tooltips.append(tooltip)
        
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
            showlegend=False
        ))
        
        # Update layout
        fig.update_layout(
            title=f"Data Lineage: {lineage_data['script_name']}",
            title_x=0.5,
            showlegend=False,
            hovermode='closest',
            margin=dict(b=20, l=5, r=5, t=40),
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            plot_bgcolor='white',
            width=1000,
            height=700
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
            fig.write_html(output_file)
            print(f"💾 Lineage visualization saved to: {output_file}")
        
        return fig

    def create_summary_dashboard(self, lineage_files: List[str], output_file: str = None) -> go.Figure:
        """Create a summary dashboard showing multiple lineage files"""
        if not lineage_files:
            print("No lineage files found")
            return None
        
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
        
        # Create subplots
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=('Script Summary', 'Table Types Distribution', 'Operation Types', 'Top Source Tables'),
            specs=[[{"type": "table"}, {"type": "pie"}],
                   [{"type": "bar"}, {"type": "bar"}]]
        )
        
        # Script Summary Table
        summary_data = []
        for data in all_data:
            summary_data.append([
                data['script_name'],
                len(data['operations']),
                len(data['source_tables']),
                len(data['target_tables']),
                len(data['volatile_tables'])
            ])
        
        fig.add_trace(
            go.Table(
                header=dict(values=['Script', 'Operations', 'Sources', 'Targets', 'Volatile']),
                cells=dict(values=list(zip(*summary_data)))
            ),
            row=1, col=1
        )
        
        # Table Types Distribution (Pie Chart)
        total_sources = sum(len(data['source_tables']) for data in all_data)
        total_targets = sum(len(data['target_tables']) for data in all_data)
        total_volatile = sum(len(data['volatile_tables']) for data in all_data)
        
        fig.add_trace(
            go.Pie(
                labels=['Source Tables', 'Target Tables', 'Volatile Tables'],
                values=[total_sources, total_targets, total_volatile],
                name="Table Types"
            ),
            row=1, col=2
        )
        
        # Operation Types (Bar Chart)
        operation_counts = {}
        for data in all_data:
            for operation in data['operations']:
                op_type = operation['operation_type']
                operation_counts[op_type] = operation_counts.get(op_type, 0) + 1
        
        fig.add_trace(
            go.Bar(
                x=list(operation_counts.keys()),
                y=list(operation_counts.values()),
                name="Operations"
            ),
            row=2, col=1
        )
        
        # Top Source Tables (Bar Chart)
        source_table_counts = {}
        for data in all_data:
            for table in data['source_tables']:
                source_table_counts[table] = source_table_counts.get(table, 0) + 1
        
        # Get top 10 source tables
        top_sources = sorted(source_table_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        
        fig.add_trace(
            go.Bar(
                x=[item[0] for item in top_sources],
                y=[item[1] for item in top_sources],
                name="Source Usage"
            ),
            row=2, col=2
        )
        
        # Update layout
        fig.update_layout(
            title="ETL Lineage Analysis Dashboard",
            title_x=0.5,
            height=800,
            showlegend=False
        )
        
        if output_file:
            fig.write_html(output_file)
            print(f"💾 Dashboard saved to: {output_file}")
        
        return fig

    def process_folder(self, folder_path: str, output_file: str = None) -> None:
        """Process all JSON files in a folder and create visualizations"""
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
        
        # Create individual visualizations
        for json_file in json_files:
            try:
                print(f"Processing: {json_file.name}")
                lineage_data = self.load_lineage_data(str(json_file))
                
                # Create visualization
                viz_file = json_file.with_suffix('.html')
                self.create_lineage_graph(lineage_data, str(viz_file))
                
            except Exception as e:
                print(f"Error processing {json_file.name}: {e}")
        
        # Create summary dashboard
        if len(json_files) > 1:
            dashboard_file = folder / "lineage_dashboard.html"
            self.create_summary_dashboard([str(f) for f in json_files], str(dashboard_file))
        
        print(f"\n✅ Processed {len(json_files)} files")
        print(f"📁 Output folder: {folder}")

    def process_single_file(self, file_path: str, output_file: str = None) -> None:
        """Process a single JSON file and create visualization"""
        try:
            lineage_data = self.load_lineage_data(file_path)
            
            if output_file is None:
                output_file = Path(file_path).with_suffix('.html')
            
            fig = self.create_lineage_graph(lineage_data, output_file)
            
            # Open in browser
            webbrowser.open(f'file://{os.path.abspath(output_file)}')
            
        except Exception as e:
            print(f"Error processing file: {e}")


def main():
    """Main function to run the lineage visualizer"""
    parser = argparse.ArgumentParser(
        description="Create interactive visualizations of ETL lineage data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process all JSON files in a folder
  python lineage_visualizer.py reports/
  
  # Process a single JSON file
  python lineage_visualizer.py reports/CAMSTAR_LOT_BONUS_sh_lineage.json
  
  # Specify output file
  python lineage_visualizer.py reports/CAMSTAR_LOT_BONUS_sh_lineage.json --output my_viz.html
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
        visualizer = LineageVisualizer()
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