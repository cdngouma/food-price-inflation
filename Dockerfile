FROM python:3.9

WORKDIR /app

# Install pip-tools to compile dependencies
RUN pip install --no-cache-dir pip-tools

# Copy the input file
COPY requirements.in .

# Compile requirements.txt
RUN pip-compile requirements.in

# Install dependencies from generated requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your code
COPY . .

# Install local packages in editable mode
RUN python -m pip install --no-cache-dir -e ./statcan_wds