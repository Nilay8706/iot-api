
import sys
import encodings
import codecs

print("Python:", sys.version)
print("Executable:", sys.executable)
print("Encodings path:", encodings.__file__)

try:
    print("IDNA codec:", codecs.lookup("idna"))
except LookupError as error:
    print("IDNA ERROR:", error)
