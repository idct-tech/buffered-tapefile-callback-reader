<?php

namespace IDCT;

use Closure;
use LogicException;
use UnexpectedValueException;

/**
 * Buffered Tapefile Callback Reader
 *
 * Allows reading of efficent (buffered) reading of tape files with an option to maintain a high precision
 * of keeping a memory regime.
 *
 * What is a tape file?
 * It is a file which progresses only in one direction: so next lines are next values, next objects,
 * highly independent from previous entries in the same file.
 *
 * A good example would be a dump of objects in a hotel and prices per stay on a particular date.
 */
class BufferedTapefileCallbackReader
{
    /**
     * Opened file's resource.
     *
     * @var Resource
     */
    protected $file;

    /**
     * Size of the lookup buffer in bytes.
     *
     * @var int
     */
    protected $buffersize;

    /**
     * Method which is executed when a desired fragment of text is caputured.
     *
     * @var Closure
     */
    protected $callback;

    /**
     * String which finishes caputring of a value.
     *
     * @var string
     */
    protected $captureEndString;

    /**
     * Length of the string which finishes caputring of a value.
     * Calculated value is stored to avoid recalculation.
     *
     * @var int
     */
    protected $captureEndStringLen;

    /**
     * The string which starts capturing of a value.
     *
     * @var string
     */
    protected $captureStartString;

    /**
     * Length of the string which starts caputring of a value.
     * Calculated value is stored to avoid recalculation.
     *
     * @var int
     */
    protected $captureStartStringLen;

    /**
     * Closes any file handle if any is open (if $file is a resource handle).
     *
     * @return $this
     */
    public function close() : self
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

        //sets the buffer
        $this->setBuffersize($bufferSize);

        return $this;
    }

    /**
     * Returns buffersize. 100000000 by default.
     *
     * @return int
     */
    public function getBuffersize() : int
    {
        return $this->buffersize;
    }

    /**
     * Sets the string which starts capturing of a value: text from the parsed file will be added to a temporary value
     * until string which identifies the end of capturing (setCaptureEndString) is found.
     *
     * @param string $string
     * @return $this
     */
    public function setCaptureStartString(string $string) : self
    {
        $this->captureStartString = $string;
        $this->captureStartStringLen = strlen($string);

        return $this;
    }

    /**
     * Gets the string which starts capturing of a value.
     *
     * @return string|null
     */
    public function getCaptureStartString() : ?string
    {
        return $this->captureStartString;
    }

    /**
     * Sets the string which finishes caputring of a value.
     *
     * @param string $string
     * @return $this
     */
    public function setCaptureEndString(string $string) : self
    {
        $this->captureEndString = $string;
        $this->captureEndStringLen = strlen($string);

        return $this;
    }

    /**
     * Gets the string which finishes caputring of a value.
     *
     * @param string $string
     * @return $this
     */
    public function getCaptureEndString()
    {
        return $this->captureEndString;
    }

    /**
     * Callback (function) which is executed when a value identified by start and end string is matched.
     * Function should accept one string argument which will should the captured value.
     *
     * @param Closure $callback
     * @return $this
     */
    public function setCallback(Closure $callback) : self
    {
        $this->callback = $callback;

        return $this;
    }

    /**
     * Main method which performs the execution of a loop which runs thru the file, looks for desired values
     * identified by capture start/end strings and executes callbacks with the values whenever there is a match.
     *
     * @throws LogicException
     * @return $this
     */
    public function run() : self
    {
        if (!is_resource($this->file)) {
            throw new LogicException("Invalid state: file not opened.");
        }

        if (empty($this->captureStartString)) {
            throw new LogicException("Invalid state: start string not set.");
        }

        if (empty($this->captureStartString)) {
            throw new LogicException("Invalid state: end string not set.");
        }
        
        //hitpoint is the "moment" when script will attempt to load additional contents into the buffer and forget
        //about the previous contents
        
        $file = $this->file;

        //now when we reach 3/4 of the buffer with offset then we load additional part
        $offset = 0;

        /**
         * 0 - looking for start
         * 1 - collecting
         * 2 - found end
         */
        $lastFound = '';
        $collecting = false;
        $buffer = $this->getNext();
        $offset = 0;

        while (true) {
            //load two in case of a partial
            $buffer .= $this->getNext();
            while (true) {
                if ($collecting === false) {
                    //nie zbieram
                    $ofStart  = $offset - $this->captureStartStringLen;
                    if ($ofStart < 0) {
                        $ofStart = 0;
                    }
                    if (($offsetStart = strpos($buffer, $this->captureStartString, $ofStart)) !== false) {
                        //znalazlem poczatek
                        if (($offsetEnd = strpos($buffer, $this->captureEndString, $offsetStart + $this->captureStartStringLen)) !== false) {
                            //znalazlem tez koniec
                            $endLenOffset = $offsetEnd + $this->captureEndStringLen;
                            $objString = substr($buffer, $offsetStart, $endLenOffset - $offsetStart);
                            call_user_func($this->callback, $objString, 0);
                            $offset = $endLenOffset;
                        } else {
                            //jednak tylko poczatek
                            $lastFound = substr($buffer, $offsetStart);
                            $offset = $offsetStart;
                            $collecting = true;
                            break;
                        }
                    } else {
                        //nic nie znalazlem
                        $offset = 0;
                        break;
                    }
                } else {
                    //zbieram
                    if (($offsetEnd = strpos($buffer, $this->captureEndString, $offset - $this->captureEndStringLen)) !== false) {
                        //mam koniec

                        $start = $offset < $this->buffersize ? $this->buffersize : $offset;
                        $lastFound .= substr($buffer, $start, $offsetEnd + $this->captureEndStringLen - $start);

                        call_user_func($this->callback, $lastFound, 1);
                        $lastFound = '';
                        $collecting = false;
                        $offset = $offsetEnd + $this->captureEndStringLen;
                    } else {
                        //nie mam konca
                        $lastFound .= substr($buffer, $this->buffersize);

                        $offset = 0;
                        break;
                    }
                }
            }

            if (feof($this->file)) {
                break;
            }
            //strip one part
            $buffer = substr($buffer, $this->getBuffersize());
            $offset -= $this->buffersize;
            if ($offset < 0) {
                $offset = 0;
            }
        }
    
        fclose($file);

        return $this;
    }

    /**
     * Loads next bytes from the file into the buffer.
     *
     * @return string
     */
    protected function getNext() : string
    {
        $c = '';
        while ($temp = fread($this->file, $this->buffersize)) {
            $c .= $temp;
            $len = strlen($c);
            if ($len >= $this->buffersize - 1) {
                break;
            }
        }

        return $c;
    }

    /**
     * Sets buffersize in bytes. At least 1KB required.
     *
     * @param int buffersize
     * @throws UnexpectedValueException
     * @return $this
     */
    protected function setBuffersize(int $buffersize) : self
    {
        $buffersize = intval($buffersize);
        if ($buffersize < 1) {
            throw new UnexpectedValueException("Buffersize should be at least 1B. Given: " . $buffersize);
        }

        $this->buffersize = $buffersize;

        return $this;
    }
}
