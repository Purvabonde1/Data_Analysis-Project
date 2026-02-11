import mysql.connector
import pandas as pd 

conn=mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="data_analysisdb"
)


df=pd.read_sql("Select* from chocolate_sales_i_",con=conn)
print(df.head())
#finding  missing value
print("Missing value:",df.isnull().sum())

# 1. Load and Clean

df['Amount'] = df['Amount'].str.replace('$', '', regex=False).str.replace(',', '', regex=False).str.strip().astype(float)
df['Date'] = pd.to_datetime(df['Date'], format='%d-%b-%y')
df['Month'] = df['Date'].dt.strftime('%Y-%m')

# 2. Key Metrics Summary
total_revenue = df['Amount'].sum()
total_boxes = df['Boxes Shipped'].sum()
avg_box_price = total_revenue / total_boxes
avg_transaction_value = df['Amount'].mean()

# 3. Monthly Sales Trend
monthly_trend = df.groupby('Month')['Amount'].sum().reset_index()

# 4. Top 5 Products by Revenue
top_products = df.groupby('Product')['Amount'].sum().sort_values(ascending=False).head(5).reset_index()

# 5. Top 5 Countries by Revenue
top_countries = df.groupby('Country')['Amount'].sum().sort_values(ascending=False).head(5).reset_index()

# 6. Top 5 Sales People (Top Performers)
top_performers = df.groupby('Sales Person')['Amount'].sum().sort_values(ascending=False).head(5).reset_index()

# 7. Correlation (Boxes vs Amount)
correlation = df['Boxes Shipped'].corr(df['Amount'])

# Output the results for the summary
print(f"Total Revenue: ${total_revenue:,.2f}")
print(f"Total Boxes: {total_boxes:,}")
print(f"Avg Price per Box: ${avg_box_price:.2f}")
print(f"Avg Transaction Value: ${avg_transaction_value:.2f}")
print("\n--- Monthly Trend ---")
print(monthly_trend)
print("\n--- Top Products ---")
print(top_products)
print("\n--- Top Countries ---")
print(top_countries)
print("\n--- Top Sales People ---")
print(top_performers)
print(f"\nCorrelation between Boxes and Amount: {correlation:.4f}")

# Save consolidated cleaning
df.to_csv('Final_Chocolate_Analysis_Cleaned.csv', index=False)
