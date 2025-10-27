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

etcd-leader:
	@$(call get-etcd-leader-cmd)

etcd-status:
	@$(call etcd-status-table-cmd)

restart-leader:
	@$(call etcd-status-table-cmd)
	@leader=$$( $(call get-etcd-leader-cmd) ); \
	echo "Leader is $$leader"; \
	docker restart $$leader
	@$(call etcd-status-table-cmd)

etcd-partition:
	@leader=$$( $(call get-etcd-leader-cmd) ); \
	echo "Leader is $$leader"; \
	docker restart $$leader; \
	docker pause $$leader; \
	sleep 5; \
	docker unpause $$leader;

etcd-leases:
	@echo "Leases:"
	@docker exec etcd1 etcdctl --endpoints=$(ETCD_ENDPOINTS) lease list | tail -n +2 | sort
	@echo ""
	@echo "Lease TTLs:"
	@docker exec etcd1 etcdctl --endpoints=$(ETCD_ENDPOINTS) lease list | \
		tail -n +2 | sort | \
		xargs -I {} docker exec etcd1 etcdctl --endpoints=$(ETCD_ENDPOINTS) lease timetolive {}

etcd-watch-leases:
	watch --interval 1 --differences "make etcd-leases"

etcd-del-leases:
	@echo "Deleting all leases..."
	@docker exec etcd1 etcdctl --endpoints=$(ETCD_ENDPOINTS) lease list | \
		tail -n +2 | \
		xargs -I {} docker exec etcd1 etcdctl --endpoints=$(ETCD_ENDPOINTS) lease revoke {} 

# Build the Rust client inside Docker
rust-build:
	docker-compose run --rm --env ETCD_ENDPOINTS=$(ETCD_ENDPOINTS) rust-client cargo build

rust-rebuild:
	docker-compose build --no-cache rust-client

rust-run:
	docker-compose run --rm --interactive --env ETCD_ENDPOINTS=$(ETCD_ENDPOINTS) rust-client cargo run

rust-stop:
	docker-compose stop rust-client

# Rebuild and run in one command
rust-restart: rust-build rust-run

# Tail logs if running in detached mode
rust-logs:
	docker logs -f rust-client
