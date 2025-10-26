# Environment variable for etcd endpoints
ETCD_ENDPOINTS=http://etcd1:2379,http://etcd2:2379,http://etcd3:2379

define get-etcd-leader-cmd
	docker exec etcd1 etcdctl \
	  --endpoints=$(ETCD_ENDPOINTS) \
	  endpoint status --write-out=json | \
	  jq -r '.[] | select(.Status.header.member_id == .Status.leader) | .Endpoint | capture("http://(?<name>[^:]+):").name'
endef


define etcd-status-table-cmd
	docker exec etcd1 etcdctl --endpoints=$(ETCD_ENDPOINTS) endpoint status --write-out=table
endef

compose:
	docker compose up -d

leader:
	@$(call get-etcd-leader-cmd)

check:
	@$(call etcd-status-table-cmd)

restart-leader:
	@$(call etcd-status-table-cmd)
	@leader=$$( $(call get-etcd-leader-cmd) ); \
	echo "Leader is $$leader"; \
	docker restart $$leader
	@$(call etcd-status-table-cmd)

partition:
	@leader=$$( $(call get-etcd-leader-cmd) ); \
	echo "Leader is $$leader"; \
	docker restart $$leader; \
	docker pause $$leader; \
	sleep 5; \
	docker unpause $$leader;

# Build the Rust client inside Docker
rust-build:
	docker-compose run --rm --env ETCD_ENDPOINTS=$(ETCD_ENDPOINTS) rust-client cargo build

rust-rebuild:
	docker-compose build --no-cache rust-client

# Run the Rust client inside Docker
rust-run:
	docker-compose run --rm --env ETCD_ENDPOINTS=$(ETCD_ENDPOINTS) rust-client cargo run

# Rebuild and run in one command
rust-restart: rust-build rust-run

# Tail logs if running in detached mode
rust-logs:
	docker logs -f rust-client
