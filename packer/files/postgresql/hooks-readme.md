# Hook directories

Hook directories live on the rootfs at `/etc/postgresql/18/main/hooks/`:

```
/etc/postgresql/18/main/hooks/
├── pre-start.d/
├── post-start.d/
├── pre-stop.d/
└── pre-fork.d/
```

`beyond-pg supervisor` runs scripts in each directory at the corresponding lifecycle point.
All directories are empty in MVP. Future tier-specific behavior lands here as drop-in scripts
without modifying the binary.

Drop scripts into the appropriate subdirectory in `packer/files/postgresql/hooks/` during
later phases; `06-beyond-pg-install.sh` installs them onto the rootfs.
