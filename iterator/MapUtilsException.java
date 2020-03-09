package iterator;
import chainexception.*;

import java.lang.*;

public class MapUtilsException extends ChainException {
  public MapUtilsException(String s){super(null,s);}
  public MapUtilsException(Exception prev, String s){ super(prev,s);}
}
