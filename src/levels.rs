//use parquet::*;
//use std::collections::hash_map::HashMap;

/*
///
/// Levels are encoded in DFS order. Each field contains number of children it has. Example:
/// a {
///    a1
///    a2
///    b {
///        b1
///    }
/// }
///
/// is encoded as:
///   vec![
///     (a, 3),
///     (a1, 0),
///     (a2, 0),
///     (b, 1),
///     (b1, 0),
///   ]
pub struct Levels<'a> {
    //root: Node<'a>
    root: HashMap<&'a String, Node<'a>>
}

struct Node<'a> {
    id: &'a String,
    children: HashMap<&'a String, Node<'a>>
}

impl<'a> Levels<'a> {
    pub fn new(meta: &'a FileMetaData) -> Levels<'a> {
        // DFS walk over flatten columns to restore hierarchical order
        let mut stack: Vec<&'a ColumnMetaData> = Vec::new();
        let mut rootMap = HashMap::new();

        {
            let mut currentMap = &rootMap;
            let columns = meta.row_groups.iter().
                flat_map(|ref g| { &g.columns }).
                flat_map(|ref c| { &c.meta_data}).
                for_each(|ref c| {stack.push(&c)});
        }

        //let root_id: String = ".".to_owned();
        Levels {
            root: rootMap,
        }
    }

    /// Given field path, find its definition level.
    pub fn definition_level(&self, path: &Vec<String>) -> u32 {
        if path.len() == 0 {return 0}

        let mut level = 0_u32;
        let mut nodes = &self.root.get();

        for id in path {
            match nodes.get(id) {
                None => return 0,
                Some(&new_level) => {
                    nodes = &new_level;
                    level += 1;
                },
            }
        }

        // not found
        return 0;
    }

    //pub fn repetition_level() -> i32 {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_levels() {

    }

}*/