let
  pkgs = import <nixpkgs> { };
  # define version
  using_python = pkgs.python312;
  # import required python packages
  required_python_packages = import ./py_requirements.nix pkgs;
  #
  nix_pyenv_directory = ".nix-pyenv";
  pyenv = using_python.withPackages required_python_packages;
in
pkgs.mkShell {
  packages = [
    pyenv
  ];
  shellHook = ''
    _SOURCE_ROOT=$(readlink -f ${builtins.toString ./.})
    if [[ $_SOURCE_ROOT == /nix/store* ]]; then
        # maybe in a flake environment
        _SOURCE_ROOT=$(readlink -f .)
    fi
    cd $_SOURCE_ROOT

    # ensure the nix-pyenv directory exists
    if [[ ! -d ${nix_pyenv_directory} ]]; then mkdir ${nix_pyenv_directory}; fi
    if [[ ! -d ${nix_pyenv_directory}/lib ]]; then mkdir ${nix_pyenv_directory}/lib; fi
    if [[ ! -d ${nix_pyenv_directory}/bin ]]; then mkdir ${nix_pyenv_directory}/bin; fi

    ensure_symlink() {
        local link_path="$1"
        local target_path="$2"
        if [[ -L "$link_path" ]] && [[ "$(readlink "$link_path")" = "$target_path" ]]; then
            return 0
        fi
        rm -f "$link_path" > /dev/null 2>&1
        ln -s "$target_path" "$link_path"
    }

    # creating python library symlinks
    for file in ${pyenv}/${using_python.sitePackages}/*; do
        basefile=$(basename $file)
        if [ -d "$file" ]; then
            if [[ "$basefile" != *dist-info && "$basefile" != __pycache__ ]]; then
                ensure_symlink "${nix_pyenv_directory}/lib/$basefile" $file
            fi
        else
            # the typing_extensions.py will make the vscode type checker not working!
            if [[ $basefile == *.so ]] || ([[ $basefile == *.py ]] && [[ $basefile != typing_extensions.py ]]); then
                ensure_symlink "${nix_pyenv_directory}/lib/$basefile" $file
            fi
        fi
    done
    for file in ${nix_pyenv_directory}/lib/*; do
        if [[ -L "$file" ]] && [[ "$(dirname $(readlink "$file"))" != "${pyenv}/${using_python.sitePackages}" ]]; then
            rm -f "$file"
        fi
    done

    # ensure the typing_extensions.py is not in the lib directory
    rm ${nix_pyenv_directory}/lib/typing_extensions.py > /dev/null 2>&1

    # add python executable to the bin directory
    ensure_symlink "${nix_pyenv_directory}/bin/python" ${pyenv}/bin/python
    export PATH=${nix_pyenv_directory}/bin:$PATH

    # prevent gc
    if command -v nix-build > /dev/null 2>&1; then
        TEMP_NIX_BUILD_COMMAND=nix-build
    else
        TEMP_NIX_BUILD_COMMAND=/run/current-system/sw/bin/nix-build
    fi
    $TEMP_NIX_BUILD_COMMAND shell.nix -A inputDerivation -o ${nix_pyenv_directory}/.nix-shell-inputs
    unset TEMP_NIX_BUILD_COMMAND
  '';
}
