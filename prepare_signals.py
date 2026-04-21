import pandas as pd
import os
from transformation_v2 import add_all_indicators_v2 as add_all_indicators
from signal_generator_v2 import generate_signal_v2

def prepare_signal_dataset(csv_path, label):
    print(f"\nPreparing signals for {label}...")

    df = pd.read_csv(csv_path)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values('timestamp').reset_index(drop=True)

    # Add indicators
    df = add_all_indicators(df)

    # Handle missing symbol column   
    if 'symbol' not in df.columns:
        df['symbol'] = label

    # Generate signal-related columns row by row
    signal_rows = []
    for i in range(len(df)):
        if i < 1:
            # Not enough rows for prev candle
            signal_rows.append({
                'timestamp': df.iloc[i]['timestamp'],
                'symbol': df.iloc[i]['symbol'],
                'match_score': 0,
                'final_signal': False,
                'logic_debug_note': 'insufficient history',
                'filters_triggered_list': [],
                'signal_combo_name': 'none'
            })
            continue

        window = df.iloc[i-1:i+1]  # Just prev + current — fast and correct
        row_signal = generate_signal_v2(window)
        signal_rows.append(row_signal)

    signal_df = pd.DataFrame(signal_rows)

    # Merge original data + indicators + signal logic
    final_df = pd.concat(
        [df.reset_index(drop=True), signal_df.reset_index(drop=True)],
        axis=1
    )

    return final_df


os.makedirs("paper_outputs", exist_ok=True)

# Prepare 1h
signals_1h = prepare_signal_dataset("ethusdt_1h_1y.csv", "ETH_1h")
signals_1h.to_csv("paper_outputs/signals_1h.csv", index=False)

print("\nDone.")
print("Saved:")
print("  - paper_outputs/signals_1h.csv")