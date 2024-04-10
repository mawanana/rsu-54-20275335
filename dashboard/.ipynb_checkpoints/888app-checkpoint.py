{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "bee28cd4",
   "metadata": {},
   "outputs": [],
   "source": [
    "from dash import Dash, html\n",
    "\n",
    "app = Dash(__name__)\n",
    "\n",
    "app.layout = html.Div([\n",
    "    html.Div(children='Hello World')\n",
    "])\n",
    "\n",
    "if __name__ == '__main__':\n",
    "    app.run(debug=True)\n"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.8.0"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
