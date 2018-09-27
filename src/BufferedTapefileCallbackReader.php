<?php
namespace IDCT;

class BufferedTapefileCallbackReader
{
    protected $file;
    protected $buffersize;
    protected $callback;

    /**
     * Closes any file handle if any is open (if $file is a resource handle).
     *
     * @return $this
     */
    public function close()
    {
        if (is_resource($this->file)) {
            fclose($this->file);
        }

        return $this;
    }

    /**
     * Opens a file. Closes any previously opened files.
     *
     * @param string $filename
     * @return $this;
     */
    public function open($filename, $bufferSize = 100000000)
    {
        //just in case we have any file open
        $this->close();

        //check if readable
        if (!is_file($filename) || !\is_readable($filename)) {
            throw new \RuntimeException('File: `' . $filename . '` is not readable.');
        }

        //TODO add exceptions
        $this->file = @fopen($filename, 'rb');
        if (!is_resource($this->file)) {
            throw new \RuntimeException('Failed to open file and acquire resource handle - file: `' . $filename . '`.');
        }

        $this->setBuffersize($bufferSize);

        return $this;
    }

    /**
     * Sets buffersize in bytes. At least 1KB required.
     *
     * @param int buffersize
     * @throws UnexpectedValueException
     * @return $this
     */
    protected function setBuffersize($buffersize) {
        $buffersize = intval($buffersize);
        if ($buffersize < 1024) {
            throw new \UnexpectedValueException("Buffersize should be at least 1KB. Given: " . $buffersize);
        }

        $this->buffersize = $buffersize;
        return $this;
    }

    /**
     * Returns buffersize. 100000000 by default.
     *
     * @return int
     */
    public function getBuffersize()
    {
        return $this->buffersize;
    }

    public function setCaptureStartString($string) {
        $this->captureStartString = $string;
        $this->captureStartStringLen = strlen($string);
        return $this;
    }

    public function getCaptureStartString() {
        return $this->captureStartString;
    }

    public function setCaptureEndString($string) {
        $this->captureEndString = $string;
        $this->captureEndStringLen = strlen($string);
        return $this;
    }

    public function getCaptureEndString() {
        return $this->captureEndString;
    }

    public function setCallback($callback) {
        $this->callback;
        return $this;
    }

    public function runReading()
    {
        $hitpoint = 0.8 * $this->getBuffersize();
        $file = fopen($filed, 'r');
        $c = $this->getNext($file, $buffersize);
        //now when we reach 3/4 of the buffer with offset then we load additional part
        $offset = 0;
        $capturesLen = $this->captureStartStringLen + $this->captureEndStringLen;
        while( ($offset = strpos($c, $this->captureStartString, $offset + 1)) !== false) {
            if ($offset > $hitpoint) {
                $offset -= $hitpoint;
                $c = substr($c, $hitpoint);
                $c .= $this->getNext($file, $buffersize);
            }
            //todo skipping
            //we look for next one:
            $offsetNext = strpos($c, $this->captureStartString, $offset + 1);
            //to check if it would be earlier than next end:
            $objEnd = strpos($c, $this->captureEndString, $offset + 1);
            //if so then it means we have an empty entry
            if ($offsetNext < $objEnd) {
                //empty entry
                continue;
            }

            $objString = substr($c, $offset, $objEnd - $offset + $capturesLen);
            call_user_func($this->callback, $objString);
        }
        fclose($file);
    }

}
