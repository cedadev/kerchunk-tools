from test_override import Parent

class Child(Parent):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def do_something(self):
        print("Something else")

if __name__ == "__main__":
    x = Child()