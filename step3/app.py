import math
from flask import Flask, request, jsonify
import math

app = Flask(__name__)

def numerical_integration(lower, upper, N):
    step = (upper - lower) / N
    result = 0
    for i in range(N):
        x = lower + i * step
        result += abs(math.sin(x)) * step
    return result

@app.route('/numericalintegralservice/<float:lower>/<float:upper>', methods=['GET'])
def integrate(lower, upper):
    lower = float(lower)
    upper = float(upper)
    result_integration = {}
    for N in [10, 100, 1000, 10000, 100000, 1000000]:
        result_integration[N] = numerical_integration(lower, upper, N)
        print(f"N={N}, Result={result_integration[N]}")
    return result_integration

if __name__ == "__main__":
    app.run(debug=True)