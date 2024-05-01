import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.naive_bayes import GaussianNB
from sklearn.metrics import mean_squared_error

def predict_player_performance(player_name, master_df):

    # Filter data for the specified player
    player_data = master_df[master_df['player'] == player_name]

    # Preprocess data
    X = player_data[['opposite_team', 'overs', 'ratio']]
    X = pd.get_dummies(X, columns=['opposite_team'], drop_first=True)
    y = player_data['ratio']

    # Split data into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Initialize and train the Naive Bayes model
    model = GaussianNB()
    model.fit(X_train, y_train)

    # Make predictions
    predictions = model.predict(X_test)

    # Evaluate the model
    mse = mean_squared_error(y_test, predictions)
    print('Mean Squared Error:', mse)
    return mse
