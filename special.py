import re

input_file = 'interface_ar_m1.dat'
output_file = 'output-ar_m1.dat'

# Read the contents of the input file
with open(input_file, 'r', encoding='ansi') as file:
    content = file.read()

# Define the pattern for abnormal special characters to be excluded

#processed_content = re.sub(r'[^\w\s@\/|()`~*$#@!-,.;:\[{("]', '-', content)

# Replace abnormal special characters with hyphens

# Write the processed content to the output file
with open(output_file, 'w', encoding='utf-8') as file:
    file.write(content)

print("Special characters replaced. Output file: " + output_file)