from datetime import datetime

# Get the current timestamp
timestamp = datetime.now()

# Convert the timestamp to a string
timestamp_str = timestamp.strftime("%Y%m%d%H%M%S")

# Generate the output filename
destfinal = "Archive/AP/interface_ap-" + timestamp_str + ".dat"

print(destfinal)
