import pandas as pd
import matplotlib.pyplot as plt

# Load GDP data
gdp = pd.read_csv('data/processed/maddison/maddison_gdp.csv')

# Filtered to show GDP for the US specifically
gdp = gdp[gdp['country'] == 'United States']
gdp.rename(columns={'gdp_pc_2011usd': 'GDP'}, inplace=True)

# Load tech breakthrough timeline
tech = pd.read_csv('src/tech_timeline.csv')

# Filters tech events to only match GDP years
tech = tech[(tech['year'] >= gdp['year'].min()) & (tech['year'] <= gdp['year'].max())]

# Plot
plt.figure(figsize=(20, 8))
plt.plot(gdp['year'], gdp['GDP'], label="Real GDP Per Capita (2011 USD)", color='blue')

# Labels
plt.xlabel("Year")
plt.ylabel("Real GDP Per Capita (2011 USD)")
plt.title("GDP Over Time with Technology Breakthroughs")

# Vertical lines and labels
for i, row in tech.iterrows():
    offset = (i % 2) * 2  # Small offset so labels don't overlap
    plt.axvline(x=row['year'], color='red', linestyle='--', alpha=0.5)
    plt.text(row['year'] + offset, gdp['GDP'].max() * 0.9, row['technology'],
             rotation=90, verticalalignment='top', fontsize=12)

# 
plt.xlim(1766, gdp['year'].max())

# Add legend and grid
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()
