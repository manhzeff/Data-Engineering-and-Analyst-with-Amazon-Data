import pandas as pd

def extract_data(filename):
    """ Extract data from a CSV file """
    try:
        data = pd.read_csv(filename)
        print("Data extraction complete.")
        return data
    except Exception as e:
        print(f"Error reading the CSV file: {e}")
        return None

def transform_data(data):
    """ Transform extracted data """
    try:
        # Handle non-numeric and unexpected strings in 'price' column before converting
        data['price'] = data['price'].replace('[\$,]', '', regex=True)
        data['price'] = pd.to_numeric(data['price'], errors='coerce')

        # Calculate mean for price and fill missing values
        mean_price = data['price'].mean()
        data['price'] = data['price'].fillna(mean_price)

        # Ensure rating is treated as string if it's not null, then convert the first part to float
        data['rating'] = data['rating'].apply(lambda x: float(x.split()[0]) if pd.notnull(x) and isinstance(x, str) else x)
        mean_rating = data['rating'].mean()
        data['rating'] = data['rating'].fillna(mean_rating)

        # Clean reviews field, convert non-digit characters, and safely convert to integer
        data['reviews'] = data['reviews'].replace('[^\d]', '', regex=True)
        data['reviews'] = pd.to_numeric(data['reviews'], errors='coerce')
        mean_reviews = data['reviews'].mean()
        data['reviews'] = data['reviews'].fillna(mean_reviews).astype(int)

        data['availability'] = data['availability'].map(lambda x: 'In Stock' in x if pd.notnull(x) else False)

        print("Data transformation complete.")
        return data
    except Exception as e:
        print(f"Error during data transformation: {e}")
        return None

def load_data(data, output_filename):
    """ Load data into a new CSV file """
    try:
        data.to_csv(output_filename, index=False)
        print("Data loaded into new CSV file successfully.")
    except Exception as e:
        print(f"Error saving the CSV file: {e}")

def main(input_filename, output_filename):
    data = extract_data(input_filename)
    if data is not None:
        transformed_data = transform_data(data)
        if transformed_data is not None:
            load_data(transformed_data, output_filename)

if __name__ == "__main__":
    input_filename = 'amazon_data.csv'
    output_filename = 'transformed_amazon_data.csv'
    main(input_filename, output_filename)
