#!/usr/bin/env python3
import json
import matplotlib.pyplot as plt
import sys
from pathlib import Path

def load_memtier_metrics(file_path):
    """Load memtier metrics from JSONL file"""
    data = []
    with open(file_path, 'r') as f:
        for line in f:
            data.append(json.loads(line))
    return data

def plot_memtier_metrics(memtier_file, output_dir='./plots'):
    """Generate memtier metric plots and save as images"""
    
    # Create output directory
    Path(output_dir).mkdir(exist_ok=True)
    
    # Load data
    memtier_data = load_memtier_metrics(memtier_file)
    
    # Separate by operation type
    totals = [m for m in memtier_data if m.get('operation') is None or m.get('operation') == 'TOTAL']
    gets = [m for m in memtier_data if m.get('operation') == 'GET']
    sets = [m for m in memtier_data if m.get('operation') == 'SET']
    
    # --- PLOT 1: Throughput (Ops/sec) ---
    fig, ax = plt.subplots(figsize=(12, 6))
    
    if totals:
        total_ops = [m.get('ops_sec', 0) for m in totals]
        ax.bar(['Total'], total_ops, color='purple', alpha=0.7, width=0.4, label='Total')
    
    if gets and sets:
        get_ops = [m.get('ops_sec', 0) for m in gets]
        set_ops = [m.get('ops_sec', 0) for m in sets]
        x = ['GET', 'SET']
        ax.bar(x, [get_ops[0] if get_ops else 0, set_ops[0] if set_ops else 0], 
               color=['green', 'orange'], alpha=0.7, width=0.4)
    
    ax.set_ylabel('Operations/sec', fontsize=12)
    ax.set_title('Memtier Throughput', fontsize=14, fontweight='bold')
    ax.grid(True, alpha=0.3, axis='y')
    plt.tight_layout()
    plt.savefig(f'{output_dir}/memtier_throughput.png', dpi=300, bbox_inches='tight')
    print(f"Saved: {output_dir}/memtier_throughput.png")
    plt.close()
    
    # --- PLOT 2: Latency Overview (Average) ---
    fig, ax = plt.subplots(figsize=(12, 6))
    
    operations = []
    latencies = []
    
    if totals and totals[0].get('latency_avg_ms') is not None:
        operations.append('Total')
        latencies.append(totals[0].get('latency_avg_ms'))
    
    if gets and gets[0].get('latency_avg_ms') is not None:
        operations.append('GET')
        latencies.append(gets[0].get('latency_avg_ms'))
    
    if sets and sets[0].get('latency_avg_ms') is not None:
        operations.append('SET')
        latencies.append(sets[0].get('latency_avg_ms'))
    
    if operations:
        colors = ['purple', 'green', 'orange'][:len(operations)]
        ax.bar(operations, latencies, color=colors, alpha=0.7, width=0.5)
        ax.set_ylabel('Latency (ms)', fontsize=12)
        ax.set_title('Average Latency by Operation', fontsize=14, fontweight='bold')
        ax.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    plt.savefig(f'{output_dir}/memtier_latency_avg.png', dpi=300, bbox_inches='tight')
    print(f"Saved: {output_dir}/memtier_latency_avg.png")
    plt.close()
    
    # --- PLOT 3: Latency Percentiles (Total) ---
    if totals:
        total_metric = totals[0]
        percentiles = []
        values = []
        
        for p in ['p50', 'p90', 'p95', 'p99', 'p99_90']:
            key = f'latency_{p}_ms'
            if key in total_metric and total_metric[key] is not None:
                percentiles.append(p.replace('_', '.'))
                values.append(total_metric[key])
        
        if percentiles:
            fig, ax = plt.subplots(figsize=(12, 6))
            ax.bar(percentiles, values, color='steelblue', alpha=0.7, width=0.5)
            ax.set_ylabel('Latency (ms)', fontsize=12)
            ax.set_xlabel('Percentile', fontsize=12)
            ax.set_title('Latency Percentiles (Total Operations)', fontsize=14, fontweight='bold')
            ax.grid(True, alpha=0.3, axis='y')
            
            # Add value labels on bars
            for i, (p, v) in enumerate(zip(percentiles, values)):
                ax.text(i, v, f'{v:.3f}ms', ha='center', va='bottom', fontsize=10)
            
            plt.tight_layout()
            plt.savefig(f'{output_dir}/memtier_latency_percentiles.png', dpi=300, bbox_inches='tight')
            print(f"Saved: {output_dir}/memtier_latency_percentiles.png")
            plt.close()
    
    # --- PLOT 4: GET vs SET Latency Comparison ---
    if gets and sets:
        fig, ax = plt.subplots(figsize=(14, 6))
        
        get_metric = gets[0]
        set_metric = sets[0]
        
        percentiles = []
        get_values = []
        set_values = []
        
        for p in ['p50', 'p90', 'p95', 'p99']:
            key = f'latency_{p}_ms'
            if key in get_metric and key in set_metric:
                percentiles.append(p.replace('_', '.'))
                get_values.append(get_metric[key])
                set_values.append(set_metric[key])
        
        if percentiles:
            x = range(len(percentiles))
            width = 0.35
            
            ax.bar([i - width/2 for i in x], get_values, width, label='GET', color='green', alpha=0.7)
            ax.bar([i + width/2 for i in x], set_values, width, label='SET', color='orange', alpha=0.7)
            
            ax.set_ylabel('Latency (ms)', fontsize=12)
            ax.set_xlabel('Percentile', fontsize=12)
            ax.set_title('GET vs SET Latency Comparison', fontsize=14, fontweight='bold')
            ax.set_xticks(x)
            ax.set_xticklabels(percentiles)
            ax.legend(fontsize=11)
            ax.grid(True, alpha=0.3, axis='y')
            
            plt.tight_layout()
            plt.savefig(f'{output_dir}/memtier_get_vs_set_latency.png', dpi=300, bbox_inches='tight')
            print(f"Saved: {output_dir}/memtier_get_vs_set_latency.png")
            plt.close()
    
    # --- PLOT 5: Bandwidth ---
    fig, ax = plt.subplots(figsize=(12, 6))
    
    operations = []
    bandwidth = []
    
    if totals and totals[0].get('kb_sec') is not None:
        operations.append('Total')
        bandwidth.append(totals[0].get('kb_sec'))
    
    if gets and gets[0].get('kb_sec') is not None:
        operations.append('GET')
        bandwidth.append(gets[0].get('kb_sec'))
    
    if sets and sets[0].get('kb_sec') is not None:
        operations.append('SET')
        bandwidth.append(sets[0].get('kb_sec'))
    
    if operations:
        colors = ['purple', 'green', 'orange'][:len(operations)]
        ax.bar(operations, bandwidth, color=colors, alpha=0.7, width=0.5)
        ax.set_ylabel('Bandwidth (KB/sec)', fontsize=12)
        ax.set_title('Bandwidth by Operation', fontsize=14, fontweight='bold')
        ax.grid(True, alpha=0.3, axis='y')
        
        # Add value labels
        for i, (op, bw) in enumerate(zip(operations, bandwidth)):
            ax.text(i, bw, f'{bw:.2f}', ha='center', va='bottom', fontsize=10)
    
    plt.tight_layout()
    plt.savefig(f'{output_dir}/memtier_bandwidth.png', dpi=300, bbox_inches='tight')
    print(f"Saved: {output_dir}/memtier_bandwidth.png")
    plt.close()
    
    # --- PLOT 6: Summary Dashboard ---
    fig = plt.figure(figsize=(16, 10))
    gs = fig.add_gridspec(3, 2, hspace=0.3, wspace=0.3)
    
    # Throughput
    ax1 = fig.add_subplot(gs[0, 0])
    if totals:
        ax1.bar(['Total Ops/sec'], [totals[0].get('ops_sec', 0)], color='purple', alpha=0.7)
        ax1.set_ylabel('Ops/sec')
        ax1.set_title('Throughput', fontweight='bold')
        ax1.grid(True, alpha=0.3, axis='y')
    
    # Bandwidth
    ax2 = fig.add_subplot(gs[0, 1])
    if totals:
        ax2.bar(['Total KB/sec'], [totals[0].get('kb_sec', 0)], color='steelblue', alpha=0.7)
        ax2.set_ylabel('KB/sec')
        ax2.set_title('Bandwidth', fontweight='bold')
        ax2.grid(True, alpha=0.3, axis='y')
    
    # Average Latency
    ax3 = fig.add_subplot(gs[1, 0])
    if operations and latencies:
        ax3.bar(operations, latencies, color=colors, alpha=0.7)
        ax3.set_ylabel('ms')
        ax3.set_title('Average Latency', fontweight='bold')
        ax3.grid(True, alpha=0.3, axis='y')
    
    # Latency Percentiles
    ax4 = fig.add_subplot(gs[1, 1])
    if totals:
        total_metric = totals[0]
        perc = []
        vals = []
        for p in ['p50', 'p95', 'p99']:
            key = f'latency_{p}_ms'
            if key in total_metric:
                perc.append(p)
                vals.append(total_metric[key])
        if perc:
            ax4.bar(perc, vals, color='coral', alpha=0.7)
            ax4.set_ylabel('ms')
            ax4.set_title('Latency Percentiles', fontweight='bold')
            ax4.grid(True, alpha=0.3, axis='y')
    
    # GET vs SET Ops/sec
    ax5 = fig.add_subplot(gs[2, 0])
    if gets and sets:
        ax5.bar(['GET', 'SET'], 
                [gets[0].get('ops_sec', 0), sets[0].get('ops_sec', 0)],
                color=['green', 'orange'], alpha=0.7)
        ax5.set_ylabel('Ops/sec')
        ax5.set_title('GET vs SET Throughput', fontweight='bold')
        ax5.grid(True, alpha=0.3, axis='y')
    
    # GET vs SET Latency
    ax6 = fig.add_subplot(gs[2, 1])
    if gets and sets:
        ax6.bar(['GET', 'SET'], 
                [gets[0].get('latency_avg_ms', 0), sets[0].get('latency_avg_ms', 0)],
                color=['green', 'orange'], alpha=0.7)
        ax6.set_ylabel('ms')
        ax6.set_title('GET vs SET Latency', fontweight='bold')
        ax6.grid(True, alpha=0.3, axis='y')
    
    fig.suptitle('Memtier Benchmark Summary', fontsize=16, fontweight='bold')
    plt.savefig(f'{output_dir}/memtier_summary_dashboard.png', dpi=300, bbox_inches='tight')
    print(f"Saved: {output_dir}/memtier_summary_dashboard.png")
    plt.close()
    
    print(f"\nAll plots saved to {output_dir}/")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python plot_memtier.py <memtier_metrics.jsonl> [output_dir]")
        sys.exit(1)
    
    memtier_file = sys.argv[1]
    output_dir = sys.argv[2] if len(sys.argv) > 2 else './plots'
    
    plot_memtier_metrics(memtier_file, output_dir)