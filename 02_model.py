import pandas as pd

from numpy import array
from keras.preprocessing.text import one_hot
from keras.preprocessing.text import Tokenizer
from keras.utils import to_categorical
from keras.models import Sequential
from keras.layers import Dense
from keras.layers import LSTM
from keras.layers import Embedding

from sklearn.model_selection import train_test_split

RANDOM_SEED = 2019
lines = pd.read_pickle("./data/02_bin/01_lines.bin")

corpus = '\n'.join(lines.line_text)

tokenizer = Tokenizer()
tokenizer.fit_on_texts([corpus])
vocab_size = len(tokenizer.word_index)+1
encoded = tokenizer.texts_to_sequences([corpus])[0]

sequences = array([
    [encoded[i], txt]
    for i, txt in enumerate(encoded[1:])
])


X = sequences[:,0]
y = to_categorical(sequences[:,1], num_classes=vocab_size)

X_train, X_test, y_train, y_test = train_test_split(
    X, y, 
    test_size=0.50, 
    random_state=RANDOM_SEED
)

model = Sequential([
    Embedding(vocab_size, 100, input_length=1),
    LSTM(100, dropout = 0.2, recurrent_dropout = 0.2),
    Dense(vocab_size, activation='softmax')
])
model.compile(
    loss='categorical_crossentropy', 
    optimizer='adam', 
    metrics=['accuracy']
)

history = model.fit(X_train, y_train, epochs=10, verbose=2)


