
df_scaled <- scale(ARR_DATA|>select(-arr24))
pca_result <- prcomp(df_scaled, center = TRUE, scale. = TRUE)
summary(pca_result)
explained_var <- pca_result$sdev^2 / sum(pca_result$sdev^2)

screeplot(pca_result, type = "lines", main = "Scree Plot")

biplot(pca_result, choices = c(1, 2), scale = 0,cex = 0, xlabs = ARR_DATA$arr24)  
biplot(pca_result, choices = c(2, 3), scale = 0,cex = 0, xlabs = ARR_DATA$arr24)  
biplot(pca_result, choices = c(3, 4), scale = 0,cex = 0, xlabs = ARR_DATA$arr24)  



ARR <- st_read("data/external/arrondissements-version-simplifiee.geojson")|>
  select(arr24=code, geometry) |>
  mutate(arr24 =paste0(substr(arr24, 1,2), substr(arr24, 5,5)))
filter(nchar(arr24)<4)|> 
  unique()

ARR <- ARR_DATA |>
  left_join(ARR, by = "arr24")

ARR|>
  ggplot(aes(geometry=geometry))+
  geom_sf(aes(fill = trafic*1000)) +
  coord_sf(crs = 2154) +
  scale_fill_distiller(palette = "Blues", direction = 1) +
  theme_minimal() +
  labs(fill="", title = "Nombre de mis en causes, trafic de stupéfiants, pour 1000")+
  theme(
    text = element_text(size=13, family="mono"),
    legend.position = "bottom", 
    panel.grid = element_blank(),
    axis.line = element_blank(),
    axis.text = element_blank(),
    legend.key.width=unit(2,"cm"), 
    plot.margin = margin(15, 0, -5, 0)
  )

ARR|>
  ggplot(aes(geometry=geometry))+
  geom_sf(aes(fill = q2_dpe)) +
  coord_sf(crs = 2154) +
  scale_fill_distiller(palette = "Blues", direction = 1) +
  theme_minimal() +
  labs(fill="", title = "DPE Médian")+
  theme(
    text = element_text(size=13, family="mono"),
    legend.position = "bottom", 
    panel.grid = element_blank(),
    axis.line = element_blank(),
    axis.text = element_blank(),
    legend.key.width=unit(2,"cm"), 
    plot.margin = margin(15, 0, -5, 0)
  )


ARR|>
  ggplot(aes(geometry=geometry))+
  geom_sf(aes(fill = n_ape5610c*1000)) +
  coord_sf(crs = 2154) +
  scale_fill_distiller(palette = "Blues", direction = 1) +
  theme_minimal() +
  labs(fill="", title = "Nombre de fast-foods, pour 1000")+
  theme(
    text = element_text(size=13, family="mono"),
    legend.position = "bottom", 
    panel.grid = element_blank(),
    axis.line = element_blank(),
    axis.text = element_blank(),
    legend.key.width=unit(2,"cm"), 
    plot.margin = margin(15, 0, -5, 0)
  )




pca_loadings <- as.data.frame(pca_result$rotation)
pca_loadings$Variable <- rownames(pca_loadings)  # Add variable names

# Scale arrows for better visualization
scale_factor <- 3  # Adjust arrow length
pca_loadings$PC1 <- pca_loadings$PC1 * scale_factor
pca_loadings$PC2 <- pca_loadings$PC2 * scale_factor


ggplot() +
  # Arrows for variable loadings
  geom_segment(data = pca_loadings, 
               aes(x = 0, y = 0, xend = PC1, yend = PC2), 
               arrow = arrow(length = unit(0.135, "inches"), type = "closed"),
               linewidth = .5) +
  
  # Variable names at arrow tips
  geom_text_repel(data = pca_loadings, 
                  aes(x = PC1, y = PC2, label = Variable), max.overlaps=100, 
                  size = 5, fontface = "bold", color = "black") +
  
  # Labels and theme
  labs(title = "PCA Biplot (Only Variables)", x = "PC1", y = "PC2") +
  theme_minimal() 
