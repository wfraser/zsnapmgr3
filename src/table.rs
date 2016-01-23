use std::fmt;

#[derive(Debug)]
pub struct Table {
    headers: Vec<String>,
    pad_left: Vec<bool>,
    items: Vec<Vec<String>>,
}

impl Table {
    pub fn new(headers: &Vec<&str>) -> Table {
        let mut table = Table {
            headers: Vec::new(),
            pad_left: Vec::new(),
            items: Vec::new(),
        };

        for heading in headers {
            if &heading[0..1] == "_" {
                table.pad_left.push(true);
                table.headers.push((&heading[1..]).to_string());
            }
            else {
                table.pad_left.push(false);
                table.headers.push(heading.to_string());
            }
        }

        table
    }

    pub fn push(&mut self, row: Vec<String>) {
        if row.len() != self.headers.len() {
            panic!("not enough values");
        }

        self.items.push(row);
    }

    /*
    pub fn append<I: Iterator<Item=Vec<String>>>(&mut self, source: &mut I) {
        for row in source {
            self.push(row);
        }
    }
    */
}

fn measure(measures: &mut Vec<usize>, row: &Vec<String>) {
    for i in 0..measures.len() {
        if row[i].len() > measures[i] {
            measures[i] = row[i].len();
        }
    }
}

fn write_measured(f: &mut fmt::Formatter, row: &Vec<String>, measures: &Vec<usize>, pad_left: &Vec<bool>) -> fmt::Result {
    for i in 0..measures.len() {
        if pad_left[i] {
            try!(write!(f, "{:>1$}", row[i], measures[i]));
        }
        else {
            try!(write!(f, "{:<1$}", row[i], measures[i]));
        }
        if i != measures.len() - 1 {
            try!(write!(f, " | "));
        }
    }
    Ok(())
}

impl fmt::Display for Table {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut measures: Vec<usize> = vec![0; self.headers.len()];

        measure(&mut measures, &self.headers);
        for row in &self.items {
            measure(&mut measures, &row);
        }

        try!(write_measured(f, &self.headers, &measures, &self.pad_left));
        try!(write!(f, "\n"));

        let mut total_measure = 0_usize;
        for i in 0..measures.len() {
            total_measure += measures[i];
            if i != measures.len() - 1 {
                total_measure += 3;
            }
        }
        try!(write!(f, "{:-<1$}\n", "-", total_measure));

        for row in &self.items {
            try!(write_measured(f, &row, &measures, &self.pad_left));
            try!(write!(f, "\n"));
        }

        write!(f, "\n")
    }
}
