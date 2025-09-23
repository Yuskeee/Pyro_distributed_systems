# saved as greeting-client.py
import Pyro5.api

name = input("What is your name? ").strip()

greeting_maker = Pyro5.api.Proxy("PYRONAME:example.greeting")    # use name server object lookup uri shortcut
print(greeting_maker.get_fortune(name))
greeting_maker.heartbeat()  # oneway call, no need to wait for a response