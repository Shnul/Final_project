import pandas as pd
import numpy as np
import psycopg2
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, classification_report
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
import logging
import os

# Database connection parameters from environment variables
DB_HOST = os.environ['DB_HOST']
DB_NAME = os.environ['DB_NAME']
DB_USER = os.environ['DB_USER']
DB_PASS = os.environ['DB_PASS']

# SendGrid API key from environment variables
SENDGRID_API_KEY = os.environ['SENDGRID_API_KEY']
FROM_EMAIL = os.environ['FROM_EMAIL']
TO_EMAIL = os.environ['TO_EMAIL']

logging.basicConfig(filename='crypto_notifier.log', level=logging.INFO, 
                    format='%(asctime)s:%(levelname)s:%(message)s')

def fetch_data():
    try:
        conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)
        query = "SELECT * FROM crypto_data;"
        data = pd.read_sql(query, conn)
    except Exception as e:
        logging.error(f"Error fetching data: {e}")
        return pd.DataFrame()
    finally:
        conn.close()
    return data

def preprocess_data(data):
    try:
        data['timestamp'] = pd.to_datetime(data['timestamp'])
        data = data.dropna()
        data['price_change'] = data['price'].pct_change()
        data['volume_change'] = data['volume'].pct_change()
        data['rolling_avg_price'] = data['price'].rolling(window=5).mean()
        data['price_momentum'] = data['price'] - data['price'].shift(5)
        data = data.dropna()
    except Exception as e:
        logging.error(f"Error preprocessing data: {e}")
        return pd.DataFrame()
    return data

def train_model(data):
    try:
        features = ['price_change', 'volume_change', 'rolling_avg_price', 'price_momentum']
        X = data[features]
        y = np.where(data['price'].shift(-1) > data['price'], 1, 0)
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        scaler = StandardScaler()
        X_train = scaler.fit_transform(X_train)
        X_test = scaler.transform(X_test)
        model = LogisticRegression()
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)
        logging.info(f"Accuracy: {accuracy_score(y_test, y_pred)}")
        logging.info(classification_report(y_test, y_pred))
    except Exception as e:
        logging.error(f"Error training model: {e}")
        return None, None
    return model, scaler

def send_notification(subject, body):
    message = Mail(
        from_email=FROM_EMAIL,
        to_emails=TO_EMAIL,
        subject=subject,
        plain_text_content=body
    )
    try:
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        response = sg.send(message)
        logging.info(f"Email sent: {response.status_code}")
    except Exception as e:
        logging.error(f"Error sending email: {e}")

def store_notification(conn, timestamp, currency, action):
    query = """
    INSERT INTO bot_notifications (timestamp, currency, action)
    VALUES (%s, %s, %s);
    """
    cursor = conn.cursor()
    cursor.execute(query, (timestamp, currency, action))
    conn.commit()

def store_prediction(conn, timestamp, currency, prediction, price, volume, bid_spread_percentage, market_cap):
    query = """
    INSERT INTO crypto_predictions (timestamp, currency, prediction, price, volume, bid_spread_percentage, market_cap)
    VALUES (%s, %s, %s, %s, %s, %s, %s);
    """
    cursor = conn.cursor()
    cursor.execute(query, (timestamp, currency, prediction, price, volume, bid_spread_percentage, market_cap))
    conn.commit()

def main():
    data = fetch_data()
    if data.empty:
        logging.error("No data fetched, exiting...")
        return

    data = preprocess_data(data)
    if data.empty:
        logging.error("Data preprocessing failed, exiting...")
        return

    model, scaler = train_model(data)
    if model is None or scaler is None:
        logging.error("Model training failed, exiting...")
        return
    
    # Fetch the latest data for real-time prediction
    latest_data = data.tail(1).iloc[0]
    latest_features = latest_data[['price_change', 'volume_change', 'rolling_avg_price', 'price_momentum']]
    latest_features_scaled = scaler.transform([latest_features])
    
    prediction = model.predict(latest_features_scaled)[0]
    prediction_label = 'buy' if prediction == 1 else 'sell'
    
    # Email content
    email_subject = f'{prediction_label.capitalize()} Signal'
    email_body = f'A {prediction_label} signal has been triggered for the cryptocurrency.\n\nDetails:\nCurrency: {latest_data["currency"]}\nPrice: {latest_data["price"]}\nVolume: {latest_data["volume"]}\nMarket Cap: {latest_data["market_cap"]}\nTime: {latest_data["timestamp"]}'

    # Send notification
    send_notification(email_subject, email_body)
    
    # Store the notification and prediction in the database
    try:
        conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)
        store_notification(conn, latest_data['timestamp'], latest_data['currency'], prediction_label)
        store_prediction(
            conn,
            latest_data['timestamp'],
            latest_data['currency'],
            prediction_label,
            latest_data['price'],
            latest_data['volume'],
            latest_data['bid_spread_percentage'],
            latest_data['market_cap']
        )
    except Exception as e:
        logging.error(f"Error storing data: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
