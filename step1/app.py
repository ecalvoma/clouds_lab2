from flask import Flask, request, render_template
import math
import socket
app = Flask(__name__)



    

def numerical_integration(lower, upper, N):
    dx = (upper - lower) / N
    result = 0.0

    for i in range(N):
        x_i = lower + i * dx
        result += abs(math.sin(x_i)) * dx

    return result

@app.route('/numericalintegralservice/<lower>/<upper>')
def integrate(lower, upper):
    lower = float(lower)
    upper = float(upper)
    integration_results = {}

    for N in [1, 10, 100, 1000, 10000, 100000, 1000000]:
        integration_results[N] = numerical_integration(lower, upper, N)

    return integration_results





if __name__ == '__main__':
    app.run(debug=True)
