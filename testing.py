from datetime import datetime

# Get the current time
current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Name of the file to be created
file_name = "output.txt"

# Write the current time to the file
with open(file_name, "w") as file:
    file.write(current_time)

print(f"File '{file_name}' has been created with the current time.")