alias path='echo -e "${PATH//:/\n}"'
#After using command source .bashrc, a shortcut to see all paths in a less cluttered way separated by newlines is available.

addtopath() {
  local target="$1"
  if [ -f "$target" ]; then
    target=$(dirname "$target")
  fi
  if [ -d "$target" ]; then
    export PATH="$PATH:$target"
  fi
}
#A local variable is used to not interfere with other potential functions in this bashrc file, or other bash processes. The first, and most likley only input is stored into target as $1. A directory or path is trying to be added to the path to function similarly to how ls functions, no extra words needed to execute. The syntax will look like 'addtopath /usr/jimmy/executables/slice' and the shell function would use checks to see if the input is a file or a directory. Files are cut off with the dirname keyword, which shows only the directory of the file. If the input was already a directory, then the first if is skipped. The shell script appends the directory to your global path variable. This script ensures files can still work if added to path on accident rather than their directory. 
