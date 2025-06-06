from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def cluster_tech_events(df, n_clusters=3, show_heatmap=True, show_pca=True):
    # Step 1: Pivot the table wide
    pivot_df = df.pivot(index='Technology', columns='Period')
    pivot_df.columns = ['_'.join(col).strip() for col in pivot_df.columns.values]
    pivot_df.reset_index(inplace=True)

    # Step 2: Prepare features
    features = pivot_df.drop('Technology', axis=1)

    # ✅ Drop rows with missing values
    features_clean = features.dropna()

    # ✅ Keep only corresponding tech rows
    pivot_df_clean = pivot_df.loc[features_clean.index].copy()

    # Step 3: Standardize features
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(features_clean)

    # Step 4: Run KMeans clustering
    kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    pivot_df_clean['Cluster'] = kmeans.fit_predict(X_scaled)

    # Step 5: Output cluster centers
    print("\n📊 Cluster Centers (standardized values):")
    centers = pd.DataFrame(kmeans.cluster_centers_, columns=features_clean.columns)
    print(centers.round(2))

    print("\n🧠 Cluster Memberships:")
    print(pivot_df_clean[['Technology', 'Cluster']].sort_values('Cluster'))

    # Step 6: Heatmap of average cluster behavior
    if show_heatmap:
        numeric_cols = pivot_df_clean.select_dtypes(include='number').columns.drop('Cluster', errors='ignore')
        cluster_avg = pivot_df_clean.groupby('Cluster')[numeric_cols].mean()

        plt.figure(figsize=(12, 6))
        sns.set_theme(style='whitegrid')
        sns.heatmap(
            cluster_avg,
            cmap='coolwarm',
            annot=True,
            fmt=".1f",
            linewidths=0.5,
            cbar_kws={'label': 'Standardized Feature Value'}
        )
        plt.title(f"Cluster Averages Across {n_clusters} Tech Clusters")
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.show()

    # Step 7: PCA 2D Scatter Plot
    if show_pca:
        pca = PCA(n_components=2)
        X_pca = pca.fit_transform(X_scaled)

        plt.figure(figsize=(10, 7))
        sns.scatterplot(
            x=X_pca[:, 0],
            y=X_pca[:, 1],
            hue=pivot_df_clean['Cluster'],
            palette='tab10',
            s=100,
            legend="full"
        )

        for i, tech in enumerate(pivot_df_clean['Technology']):
            plt.text(X_pca[i, 0]+0.02, X_pca[i, 1]+0.02, tech, fontsize=9)

        plt.title("PCA Scatterplot of Technologies Colored by Cluster")
        plt.xlabel("PCA 1")
        plt.ylabel("PCA 2")
        plt.grid(True)
        plt.tight_layout()
        plt.show()

    return pivot_df_clean
    #To run(    clustered_df = cluster_tech_events(df,n_clusters=3)    )
