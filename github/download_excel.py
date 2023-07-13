import requests

# Define the SharePoint file's shareable link
sharepoint_link = 'https://vacoss-my.sharepoint.com/:x:/g/personal/matt_garriga_morgan-franklin_com/Eb8DEdx3QrlDl_zTiwYReaABpQwJVQpf0cRbITupEBPNpA'

# Define the destination file path to save the downloaded file
destination = 'xlsx.xlsx'

# Download the file from SharePoint
response = requests.get(sharepoint_link)

# Save the downloaded file to the specified destination
with open(destination, 'wb') as file:
    file.write(response.content)