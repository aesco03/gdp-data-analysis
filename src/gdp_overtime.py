#!/usr/bin/env python
# coding: utf-8

# In[29]:


import pandas as pd
import matplotlib.pyplot as plt

#Load GDP data
gdp = pd.read_csv('data/processed/GPDIC1_normalized.csv')

gdp['date'] = pd.to_datetime(gdp['date'])
gdp['year'] = gdp['date'].dt.year
gdp.rename(columns={'Gross_Private_Investment_real': 'GDP'}, inplace=True)

#Load Technology Timeline
tech = pd.read_csv('src/tech_timeline.csv')



#Filters tech events to only match GDP years
tech = tech[(tech['year'] >= gdp['year'].min()) & (tech['year'] <= gdp['year'].max())]

#Plot
plt.figure(figsize=(20, 8))
plt.plot(gdp['year'], gdp['GDP'], label="Real GDP", color='blue')

#Labels
plt.xlabel("Year")
plt.ylabel("Real GDP (Chained 2012 Dollars)")
plt.title("GDP Over Time with Technology Breakthroughs")

# Vertical lines and labels
for i, row in tech.iterrows():
    offset = (i % 2) * 2  # Small offset so labels don't overlap
    plt.axvline(x=row['year'], color='red', linestyle='--', alpha=0.5)
    plt.text(row['year'] + offset, gdp['GDP'].max() * 0.9, row['technology'],
             rotation=90, verticalalignment='bottom', fontsize=7)

# Add legend and grid
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()



