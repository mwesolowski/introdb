sync
sudo sysctl vm.drop_caches=1
sudo sysctl vm.dirty_ratio=80
sudo sysctl vm.dirty_background_ratio=80

sudo sysctl kernel.perf_event_paranoid=-1
