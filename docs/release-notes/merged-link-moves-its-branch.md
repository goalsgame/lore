---
type: fix
---

- `lore-revision`: Fixed merging a branch that adds a link leaving the linked repository's branch where it was, after
  which every later merge into the parent failed with `Branch push is not a fast-forward` on the link. The push now
  brings the linked branch to the link's pin, and a linked branch an earlier push left behind catches up on the next
  merge's push
