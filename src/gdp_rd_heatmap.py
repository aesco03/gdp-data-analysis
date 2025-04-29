import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# --- Load GDP Data ---
gdp = pd.read_csv('data/processed/maddison_gdp.csv')
gdp = gdp[gdp['country'] == 'United States'].copy()
gdp.rename(columns={'gdp_pc_2011usd': 'GDP'}, inplace=True)
gdp['GDP_growth'] = gdp['GDP'].pct_change() * 100
gdp['year'] = gdp['year'].astype(int)

# --- Load R&D Data ---
private_rd = pd.read_csv('data/processed/Y006RC1Q027SBEA_normalized.csv')
private_rd['year'] = pd.to_datetime(private_rd['date']).dt.year

gov_rd = pd.read_csv('data/processed/Y057RC1Q027SBEA_normalized.csv')
gov_rd['year'] = pd.to_datetime(gov_rd['date']).dt.year

# --- Merge GDP + Private R&D + Gov R&D ---
merged = gdp.merge(private_rd[['year', 'Private_RD_real']], on='year', how='left')
merged = merged.merge(gov_rd[['year', 'Gov_RD_real']], on='year', how='left')

# --- Load Tech Timeline ---
tech = pd.read_csv('src/tech_timeline.csv')
tech = tech[(tech['year'] >= merged['year'].min()) & (tech['year'] <= merged['year'].max())]

# --- Build Before/During/After Table ---
records = []

for _, row in tech.iterrows():
    y = row['year']
    tech_name = row['technology']

    for offset, label in zip([-1, 0, 1], ['Before', 'During', 'After']):
        year = y + offset
        data = merged[merged['year'] == year]

        if not data.empty:
            records.append({
                'Technology': tech_name,
                'Period': label,
                'Year': year,
                'GDP_growth': data['GDP_growth'].values[0],
                'Private_RD': data['Private_RD_real'].values[0],
                'Gov_RD': data['Gov_RD_real'].values[0]
            })

df = pd.DataFrame(records)

# --- Create Pivot Tables ---
gdp_pivot = df.pivot(index='Technology', columns='Period', values='GDP_growth')
private_pivot = df.pivot(index='Technology', columns='Period', values='Private_RD')
gov_pivot = df.pivot(index='Technology', columns='Period', values='Gov_RD')

# --- Reorder technologies chronologically ---
tech_order = tech.sort_values('year')['technology']
gdp_pivot = gdp_pivot.loc[tech_order]
private_pivot = private_pivot.loc[tech_order]
gov_pivot = gov_pivot.loc[tech_order]

# --- Plot Side-by-Side Heatmaps ---
fig, axes = plt.subplots(1, 3, figsize=(22, 8))

sns.set_theme(style='whitegrid')

# --- GDP Growth Heatmap ---
sns.heatmap(
    gdp_pivot,
    cmap='coolwarm',
    center=0,
    annot=True,
    fmt=".1f",
    linewidths=0.5,
    cbar_kws={'label': 'GDP Growth (%)'},
    ax=axes[0]
)
axes[0].set_title('GDP Growth')
axes[0].set_ylabel('Technology')
axes[0].set_xlabel('Period')

# --- Private R&D Heatmap ---
sns.heatmap(
    private_pivot,
    cmap='YlGnBu',
    annot=True,
    fmt=".0f",
    linewidths=0.5,
    cbar_kws={'label': 'Private R&D (Real $)'},
    ax=axes[1]
)
axes[1].set_title('Private R&D Investment')
axes[1].set_ylabel('')
axes[1].set_xlabel('Period')

# --- Government R&D Heatmap ---
sns.heatmap(
    gov_pivot,
    cmap='YlOrRd',
    annot=True,
    fmt=".0f",
    linewidths=0.5,
    cbar_kws={'label': 'Gov R&D (Real $)'},
    ax=axes[2]
)
axes[2].set_title('Government R&D Investment')
axes[2].set_ylabel('')
axes[2].set_xlabel('Period')

plt.tight_layout()
plt.show()
