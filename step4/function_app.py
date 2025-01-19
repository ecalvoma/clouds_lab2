import azure.functions as func

app = func.FunctionApp()

@app.function_name(name="numerical_integration")
@app.route(route="numerical_integration")
def numerical_integration(req: func.HttpRequest) -> func.HttpResponse:
    # Your numerical integration logic here
    lower = float(req.params.get('lower'))
    upper = float(req.params.get('upper'))

    # Example integration logic
    def f(x):
        return abs(math.sin(x))

    N = 1000  # Number of subintervals
    dx = (upper - lower) / N
    integral = sum(f(lower + i * dx) * dx for i in range(N))

    return func.HttpResponse(f"Numerical integration result: {integral}")