# Author: Zachary Starr
# Date: 3/24/2026

"""

A mini pipeline to process CLO data end-to-end. First, a raw Excel sheet stores about 27 rows of data (tiny).
Interest rates and risk levels are then calculated, and the data is processed and stored in a database. From there,
two reports are produced and a dashboard to go along with them.

Uses Data pipeline and ETL, Data analysis and financial calculations, reporting and visualizations

"""

# libraries needed
import pandas as pd
import sqlite3
import os
import matplotlib.pyplot as plt

# Converts a single Excel sheet to a CSV file, sheet_name is the name of the sheet in Excel
def convert_file(excel_file, csv_file, sheet_name="CLOdata"):
    try:
        # Read the Excel file into a pandas dataframe
        df = pd.read_excel(excel_file, sheet_name=sheet_name)

        # Convert the dataframe to a CSV file
        df.to_csv(csv_file, index=False, encoding='utf-8')
        print(f"\nSuccessfully converted '{excel_file}' (Sheet: {sheet_name}) to '{csv_file}'")

    # Throw an exception if unable to write the file into a df and convert
    except Exception as e:
        print(f"An error occured: {e}")

# Calculate monthly interest
def calculate_monthly_interest(df):
    return (df["principal"] * df["interest_rate"]) / 12

# Calculate total interest (uses monthly_interest derived from calculate_monthly_interest)
def calculate_total_interest(df):
    return df["monthly_interest"] * df["term_months"]

# Classify risk levels based on the tranche ID
def classify_risks(tranche):
    # if "Equity", automatically a high risk
    if tranche["tranche_id"] == "Equity":
        return "High"

    # High-risk ratings to consider -> all other ratings are considered low ("AAA", "AA", "A", "BBB")
    high_risk_ratings = ["BB", "B", "Unrated"]

    # if the tranche_rating is in the list, return high, else low (.strip() ensures extra spaces don't break the data)
    if tranche["tranche_rating"].strip() in high_risk_ratings:
        return "High"
    else:
        return "Low"

# Process the key metrics calculated from the dataframe
def process_data(csv_file):
    # load the CSV into a dataframe
    df = pd.read_csv(csv_file)

    # Calculate monthly and total interest
    df["monthly_interest"] = calculate_monthly_interest(df)
    df["total_interest"] = calculate_total_interest(df)

    # Classify the risk level
    df["risk_level"] = df.apply(classify_risks, axis=1)

    return df

########################################### Separates the SQL ###########################################

# Database storage uses sqlite3
def store_in_database(df):
    db_name = "CLO.db"

    connection = sqlite3.connect(db_name)

    df.to_sql("CLO_Loans", connection, if_exists="replace", index=False)
    print(f"\nData stored in SQLite database titled, '{db_name}'")

    return connection

# Tranche-level report on loan count, total principal, average rate, and equity %
def generate_tranche_report(connection):
    query = """
    SELECT
        tranche_id,
        COUNT(*) as loan_count,
        SUM(principal) as total_principal,
        AVG(interest_rate) as avg_interest_rate,
        SUM(equity_percent) as total_equity_percent
    FROM CLO_loans
    GROUP By tranche_id
    """

    report = pd.read_sql(query, connection)

    pd.set_option("display.max_columns", None)
    pd.set_option('display.width', 1000)
    pd.set_option('display.float_format', '{:,.2f}'.format)

    print("\n----------------------------- CLO Report by Tranche ------------------------------ \n")
    print(report)

# Risk Level Summary report on risk level, total principal, total interest, and loan count
def generate_risk_summary(connection):
    query = """
    SELECT
        tranche_id,
        risk_level,
        COUNT(*) as loan_count,
        SUM(principal) as total_principal,
        ROUND(SUM(total_interest), 2) as total_interest
    FROM CLO_loans
    GROUP BY tranche_id, risk_level
    ORDER BY
        CASE risk_level
        WHEN 'High' THEN 1
        ELSE 2
    END,
    tranche_id
    """

    report = pd.read_sql(query, connection)

    print("\n--------------------- CLO Summary Risk Report ---------------------- \n")
    print(report)

# dashboard to showcase risk level summary
def generate_risk_dashboard(connection):
    query = """
    SELECT
        tranche_id,
        risk_level,
        SUM(principal) as total_principal,
        ROUND(SUM(total_interest), 2) as total_interest
    FROM CLO_loans
    GROUP BY tranche_id, risk_level
    ORDER BY
        CASE risk_level
        WHEN 'High' THEN 1
        ELSE 2
    END,
    tranche_id
    """

    report = pd.read_sql(query, connection)

    report["total_interest"] = pd.to_numeric(report["total_interest"], errors='coerce')

    # Pivot for easier plotting
    pivot_interest = report.pivot(index='tranche_id', columns='risk_level', values='total_interest').fillna(0)

    # Plot total interest exposure
    pivot_interest.plot(kind='bar', stacked=True, figsize=(10, 6), color=['red', 'green'])
    plt.title("CLO Total Interest Exposure by Tranche & Risk Level")
    plt.ylabel("Total Interest ($)")
    plt.xlabel("Tranche ID")
    plt.xticks(rotation=45)
    plt.legend(title="Risk Level")
    plt.tight_layout()
    plt.show()

# main
if __name__ == "__main__":
    # obtain the base directory
    base_directory = os.path.dirname(os.path.dirname(__file__))

    # Excel file path and csv file path
    excel_path = os.path.join(base_directory, "data", "CLOdata.xlsx")
    csv_path = os.path.join(base_directory, "data", "CLOData.csv")

    # Convert the Excel sheet to a CSV file to start
    convert_file(excel_path, csv_path)

    # Process the key metrics and classify risks next
    df = process_data(csv_path)

    # Store the key metrics and risks in a database
    connection = store_in_database(df)

    # Generate a tranche-level report to showcase
    generate_tranche_report(connection)

    # Generate a summary risk report to showcase
    generate_risk_summary(connection)

    # Bring together the summary risk report to showcase a clean dashboard showing total interest exposure by tranche and risk level
    generate_risk_dashboard(connection)