class DataAggregator:
    def __init__(self):
        self.data_storage = {}
        self.new_data = False

    def aggregate(self, topic, message):
        # Extract location and device from the topic
        parts = topic.split('/')
        location = '/'.join(parts[1:3])  # e.g., Porto/MeetingRoom
        device_id = parts[3]  # e.g., Light1

        current_energy = message['aenergy']['total']

        # Initialize location if not already present
        if location not in self.data_storage:
            self.data_storage[location] = {'total_energy': 0, 'devices': {}}

        # Check if device already has data
        if device_id in self.data_storage[location]['devices']:
            # Calculate the difference in energy since last message
            last_energy = self.data_storage[location]['devices'][device_id]['last_energy']
            energy_diff = current_energy - last_energy
            self.data_storage[location]['total_energy'] += energy_diff
        else:
            # Initialize device; assume zero energy consumed before this point
            energy_diff = 0  # No previous message, so no diff calculation

        # Update the device's last known energy
        self.data_storage[location]['devices'][device_id] = {
            'last_energy': current_energy,
            'total_energy': energy_diff
        }
        # * Mark we have new data to publish 
        self.new_data = True

    def get_aggregated_data(self, location):
        return self.data_storage.get(location, {})

    def has_new_data(self):
        return self.new_data

    def clear_new_data_flag(self):
        self.new_data = False
