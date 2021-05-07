
release-patch:
    bumpver update -n --tag final

release-minor:
    bumpver update --minor -n --tag final

release-major:
    bumpver update --major -n --tag final

patch-beta:
    bumpver update --patch --tag beta -n
