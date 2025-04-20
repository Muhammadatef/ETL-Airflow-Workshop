import pandas as pd
import numpy as np

def generate_customer_demographics():
    """Generate sample customer demographic data"""
    n_records = 1000
    
    demographics = {
        'customer_id': range(1, n_records + 1),
        'Age_Band': np.random.choice(['18-24', '25-34', '35-44', '45-54', '55+'], n_records),
        'Gender': np.random.choice(['M', 'F'], n_records),
        'Home_Emirate': np.random.choice(['Dubai', 'Abu Dhabi', 'Sharjah', 'Ajman', 'RAK'], n_records),
        'Home_Community': np.random.choice(['Downtown', 'Marina', 'JBR', 'Silicon Oasis', 'Palm Jumeirah'], n_records),
        'Nationality': np.random.choice(['UAE', 'Indian', 'Pakistani', 'British', 'American'], n_records),
        'Marital_Status': np.random.choice(['Single', 'Married', 'Divorced'], n_records),
        'Monthly_Income': np.random.choice([
            'Below 5000 AED', '5000-10000 AED', '10001-20000 AED', 
            '20001-30000 AED', '30001-50000 AED', 'Above 50000 AED'
        ], n_records),
        'Education_Level': np.random.choice([
            'High School', 'Diploma', 'Bachelors', 'Masters', 'PhD'
        ], n_records),
        'Occupation_Sector': np.random.choice([
            'Government', 'Private Sector', 'Self-Employed', 
            'Healthcare', 'Education', 'Technology', 'Finance', 
            'Retail', 'Construction'
        ], n_records),
        'Residence_Type': np.random.choice([
            'Apartment', 'Villa', 'Townhouse', 'Shared Accommodation'
        ], n_records),
        'Years_in_UAE': np.random.choice([
            'Less than 1 year', '1-3 years', '3-5 years', 
            '5-10 years', 'More than 10 years', 'UAE National'
        ], n_records)
    }
    
    df = pd.DataFrame(demographics)
    df.to_csv('/home/maf/AI_Marketing/customer_demographics_python.csv', index=False)
    print("✅ Customer demographics data saved!")

def generate_usage_patterns():
    """Generate sample usage pattern data"""
    n_records = 1000
    
    data_consumption_weights = [0.3, 0.5, 0.2]  
    call_consumption_weights = [0.25, 0.5, 0.25]  
    
    usage = {
        'customer_id': range(1, n_records + 1),
        'Data_Consumption': np.random.choice(['Low', 'Mid', 'High'], n_records, p=data_consumption_weights),
        'Call_Consumption': np.random.choice(['Low', 'Mid', 'High'], n_records, p=call_consumption_weights),
        'International_Calling': np.random.choice(['Yes', 'No'], n_records),
        'Device_Type': np.random.choice(['iPhone', 'Samsung', 'Other'], n_records, p=[0.35, 0.40, 0.25]),
        'Social_Media_Usage': np.random.choice(['High', 'Medium', 'Low'], n_records),
        'Roaming_Usage': np.random.choice(['Frequent', 'Occasional', 'Never'], n_records),
        'Peak_Usage_Time': np.random.choice(['Morning', 'Afternoon', 'Evening', 'Night'], n_records),
        'Video_Streaming': np.random.choice(['High', 'Medium', 'Low'], n_records),
        'Gaming_Usage': np.random.choice(['Yes', 'No'], n_records),
        'Messaging_Apps_Usage': np.random.choice(['High', 'Medium', 'Low'], n_records),
        'Voice_Call_Quality': np.random.choice(['Excellent', 'Good', 'Fair', 'Poor'], n_records, p=[0.4, 0.3, 0.2, 0.1])
    }
    
    df = pd.DataFrame(usage)
    df.to_csv('/home/maf/AI_Marketing/usage_patterns_python.csv', index=False)
    print("✅ Usage patterns data saved!")

def generate_lifestyle_indicators():
    """Generate sample lifestyle indicator data"""
    n_records = 1000
    
    lifestyle = {
        'customer_id': range(1, n_records + 1),
        'Food_Lovers': np.random.choice(['Yes', 'No'], n_records),
        'Music_Lovers': np.random.choice(['Yes', 'No'], n_records),
        'Sports_Lovers': np.random.choice(['Yes', 'No'], n_records),
        'Fashion_Lovers': np.random.choice(['Yes', 'No'], n_records),
        'Travel_Frequency': np.random.choice(['Frequent', 'Occasional'], n_records)
    }
    
    df = pd.DataFrame(lifestyle)
    df.to_csv('/home/maf/AI_Marketing/lifestyle_indicators_python.csv', index=False)
    print("✅ Lifestyle indicators data saved!")

def process_and_merge_data():
    """Merge all three data sources and save final dataset"""
    print("🔄 Merging datasets...")

    demographics = pd.read_csv('/home/maf/AI_Marketing/customer_demographics_python.csv')
    usage = pd.read_csv('/home/maf/AI_Marketing/usage_patterns_python.csv')
    lifestyle = pd.read_csv('/home/maf/AI_Marketing/lifestyle_indicators_python.csv')
    
    merged_df = demographics.merge(usage, on='customer_id').merge(lifestyle, on='customer_id')

    merged_df['Income_Category'] = np.random.choice(
        ['High Income Earners', 'Middle Income Earners', 'Low Income Earners'], len(merged_df)
    )
    
    merged_df['Socio_Economic_Segment'] = np.random.choice(
        ['Premium', 'Mass Market', 'Value Seekers'], len(merged_df)
    )
    
    merged_df.to_csv('/home/maf/AI_Marketing/processed_customer_data_python.csv', index=False)
    print("✅ Final processed customer data saved!")

if __name__ == "__main__":
    generate_customer_demographics()
    generate_usage_patterns()
    generate_lifestyle_indicators()
    process_and_merge_data()

