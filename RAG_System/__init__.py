from .Retrieval.response_generator import ResponseGenerator

__all__ = [
    "ResponseGenerator",
]

__version__ = "0.1.0"

generator = ResponseGenerator()
response1 = generator.generate_response("Tình trạng rạn san hô ở Hòn Mun?")
print(response1)