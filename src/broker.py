# The broker. One listener on port 5000 or 5001 if ur in mac 
# one thread per connection.
# The order that matters: save the event first, then send it out. And never
# hold a lock while sending — copy what you need, let go, then send.
