# dbc_datasus.md

## DataSUS Data Sources

```rust
    ///decompress.rs
    use super::error::{Error, Result};
    use explode::ExplodeReader;
    use std::fs::File;
    use std::fs::OpenOptions;
    use std::io;
    use std::io::Chain;
    use std::io::Cursor;
    use std::io::Read;
    use std::path::Path;

    type DbfReader<R> = Chain<Chain<Cursor<[u8; 10]>, Cursor<Vec<u8>>>, ExplodeReader<R>>;

    /// Decompress a .dbc file into a .dbf file
    pub fn decompress<P>(dbc_path: P, dbf_path: P) -> Result<()>
    where
        P: AsRef<Path>,
    {
        let dbc_file = File::open(dbc_path)?;
        let mut dbf_reader = into_dbf_reader(dbc_file)?;
        let mut dbf_file = OpenOptions::new().write(true).create(true).open(dbf_path)?;
        io::copy(&mut dbf_reader, &mut dbf_file)?;

        Ok(())
    }

    /// Transform a .dbc reader into a .dbf reader. Make sure `dbc_reader` starts at the beginning of the file.
    pub fn into_dbf_reader<R>(mut dbc_reader: R) -> Result<DbfReader<R>>
    where
        R: io::Read,
    {
        let mut pre_header: [u8; 10] = Default::default();
        let mut crc32: [u8; 4] = Default::default();
        dbc_reader
            .read_exact(&mut pre_header)
            .map_err(|_| Error::MissingHeader)?;

        let header_size: usize = usize::from(pre_header[8]) + (usize::from(pre_header[9]) << 8);

        let mut header: Vec<u8> = vec![0; header_size - 10];
        dbc_reader
            .read_exact(&mut header)
            .map_err(|_| Error::InvalidHeaderSize)?;
        dbc_reader
            .read_exact(&mut crc32)
            .map_err(|_| Error::InvalidHeaderSize)?;

        // Create readers for each part of the file
        let pre_header_reader = Cursor::new(pre_header);
        let header_reader = Cursor::new(header);
        let compressed_content_reader = ExplodeReader::new(dbc_reader);

        let dbf_reader = pre_header_reader
            .chain(header_reader)
            .chain(compressed_content_reader);

        Ok(dbf_reader)
    }

#[cfg(test)]
    mod tests {
        use std::fs;

        use super::*;

        #[test]
        fn test_decompress() -> Result<()> {
            let input = r"test\data\sids.dbc";
            let output = r"test\data\sids.dbf";
            let expected = r"test\data\expected-sids.dbf";

            decompress(input, output)?;

            let output_file = fs::read(output)?;
            let expected_file = fs::read(expected)?;
            fs::remove_file(output)?;

            assert_eq!(
                output_file, expected_file,
                "Decompressed .dbf is not equal to expected result"
            );

            Ok(())
        }
    }
```

```rust
    ///error.rs
#[derive(Debug)]
    pub enum Error {
        /// An IO error
        Io(std::io::Error),
        /// Error while decompressing the content of the file
        Decompression(explode::Error),
        /// File without dbc header
        MissingHeader,
        /// Header size is greater than the file size
        InvalidHeaderSize,
    }

    /// Result type from reading a dbc file
    pub type Result<T> = std::result::Result<T, Error>;

    impl std::convert::From<std::io::Error> for Error {
        fn from(err: std::io::Error) -> Self {
            Error::Io(err)
        }
    }

    impl std::fmt::Display for Error {
        fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            match self {
                Error::Io(err) => write!(f, "{}", err),
                Error::Decompression(err) => write!(f, "{}", err),
                Error::MissingHeader => write!(f, "file does not contain dbc header or is empty"),
                Error::InvalidHeaderSize => write!(f, "dbc header size is greater than the file size"),
            }
        }
    }

    impl std::error::Error for Error {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            match self {
                Error::Io(err) => Some(err),
                Error::Decompression(err) => Some(err),
                _ => None,
            }
        }
    }
```

````rust
    ///lib.rs
    //! Decompress `*.dbc` files usually found in Brazil's DATASUS [ftp server] into `*.dbf` files.
    //!
    //! The underlying decompression algorithm used in `*.dbc` files is the *implode* algorithm from the PKWARE Data Compression Library.
    //! This library uses *Aaron Griffith*'s [rust implementation] of the *implode* algorithm. Also,
    //! this library is heavily inspired by *Daniela Petruzalek*'s [pysus]. I want to thank both of them, without their work this library
    //! would not be possible.
    //!
    //! [ftp server]: ftp://ftp.datasus.gov.br/dissemin/publicos
    //! [rust implementation]: https://crates.io/crates/explode
    //! [pysus]: https://github.com/danicat/pysus
    //!
    //! # Examples
    //!
    //! To decompress a `*.dbc` file into a `*.dbf` use [`decompress`](fn.decompress.html):
    //! ```no_run
    //! datasus_dbc::decompress("input.dbc", "output.dbf");
    //! ```
    //!
    //! ---
    //!
    //! If you want more control over how the `*.dbc` file is read,
    //! you can pass a [`File`][File] or other type which implements [`Read`][Read] to [`into_dbf_reader`](fn.into_dbf_reader.html)
    //! to get a reader of the decompressed content.
    //! ```no_run
    //! use std::io::Read;
    //!
    //! let dbc_file = std::fs::File::open("input.dbc").unwrap();
    //! let mut dbf_reader = datasus_dbc::into_dbf_reader(dbc_file).unwrap();
    //! let mut buf: Vec<u8> = Default::default();
    //! dbf_reader.read_to_end(&mut buf).unwrap();
    //! println!("{:?}", &buf[0..20]);
    //! ```
    //!
    //! [Read]: https://doc.rust-lang.org/std/io/trait.Read.html
    //! [File]: https://doc.rust-lang.org/std/io/struct.File.html
    //!

    mod decompress;
    mod error;

    pub use decompress::{decompress, into_dbf_reader};
    pub use error::{Error, Result};
````
