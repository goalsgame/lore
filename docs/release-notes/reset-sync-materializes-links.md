---
type: fix
---

- `lore-revision`: Fixed `lore sync --reset` onto a revision that adds a link leaving the link's directory missing,
  and onto one that drops a link leaving its files behind, including links that a linked repository's new pin adds or
  drops, and a tracked directory replaced by a link or a link by a directory. A dry run of such a sync no longer
  writes the added link's files
