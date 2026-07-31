use std::io::{self, Read, Write};
use std::os::unix::net::UnixStream;
use std::time::{Duration, Instant};

use mio::net::UnixStream as MioUnixStream;
use mio::{Events, Interest, Poll, Token};

const IO_TOKEN: Token = Token(0);

pub(crate) struct DeadlineUnixStream {
    stream: MioUnixStream,
    poll: Poll,
    events: Events,
    timeout: Duration,
    read_deadline: Option<Instant>,
    write_deadline: Option<Instant>,
}

impl DeadlineUnixStream {
    pub(crate) fn new(stream: UnixStream, timeout: Duration) -> io::Result<Self> {
        stream.set_nonblocking(true)?;
        let mut stream = MioUnixStream::from_std(stream);
        let poll = Poll::new()?;
        poll.registry()
            .register(&mut stream, IO_TOKEN, Interest::READABLE)?;
        Ok(Self {
            stream,
            poll,
            events: Events::with_capacity(4),
            timeout,
            read_deadline: None,
            write_deadline: None,
        })
    }

    pub(crate) fn begin_read(&mut self) -> io::Result<()> {
        self.read_deadline = Some(deadline_after(self.timeout)?);
        Ok(())
    }

    pub(crate) fn begin_write(&mut self) -> io::Result<()> {
        self.write_deadline = Some(deadline_after(self.timeout)?);
        Ok(())
    }

    fn remaining(deadline: Option<Instant>) -> io::Result<Duration> {
        let deadline = deadline.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::TimedOut,
                "Unix socket deadline is unavailable",
            )
        })?;
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "Unix socket deadline expired",
            ))
        } else {
            Ok(remaining)
        }
    }

    fn wait(&mut self, interest: Interest, deadline: Option<Instant>) -> io::Result<()> {
        self.poll
            .registry()
            .reregister(&mut self.stream, IO_TOKEN, interest)?;
        loop {
            let remaining = Self::remaining(deadline)?;
            self.events.clear();
            match self.poll.poll(&mut self.events, Some(remaining)) {
                Ok(()) if self.events.iter().any(|event| event.token() == IO_TOKEN) => {
                    return Ok(());
                }
                Ok(()) => {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "Unix socket deadline expired",
                    ));
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
    }
}

impl Read for DeadlineUnixStream {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        loop {
            match self.stream.read(buffer) {
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    self.wait(Interest::READABLE, self.read_deadline)?;
                }
                result => return result,
            }
        }
    }
}

impl Write for DeadlineUnixStream {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        loop {
            match self.stream.write(buffer) {
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    self.wait(Interest::WRITABLE, self.write_deadline)?;
                }
                result => return result,
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        loop {
            match self.stream.flush() {
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    self.wait(Interest::WRITABLE, self.write_deadline)?;
                }
                result => return result,
            }
        }
    }
}

fn deadline_after(timeout: Duration) -> io::Result<Instant> {
    Instant::now().checked_add(timeout).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "Unix socket deadline is invalid",
        )
    })
}
