# Publishing Conda Libraries

This repository template is setup to publish a Python package into Foundry.

By default, the repository name at the time of the bootstrap is used as the name of the conda package.
To override this, modify the ``condaPackageName`` variable in the ``gradle.properties`` file. You will have to tick
the "Show hidden files" checkbox located in the bottom left of the Authoring interface.
 
The ``build.gradle`` file configures the publish task to only run when the repository is tagged.
You can create tag in "Branches" tab in Authoring.

Each Authoring repository has associated an Artifacts repository which holds all the produced packages. 
Libraries can currently be published to Artifacts repository (default) or to the shared channel.
In the future, libraries will always be published to Artifacts repository.
**If you want your library to be available in Code Workbooks, you will have to publish it to the shared channel.**

**If you decide to publish a library to the shared channel, it is not safe to go back to using Artifacts repository!** 

To publish your library to the shared channel, you have to add following in ``build.gradle``:

```
pythonLibrary {	
    publishChannelName = 'libs'	
}
```

By publishing a library to the shared channel, your repository will acquire ownership over the name of that library.
This means that if another repository tries to publish a library of the same name to the channel, it will be rejected.
This is done to prevent conflicts.

## Consuming Conda Libraries

If a library is published to Artifacts repository (default), you will need read access on the library's origin repository to be able to 
consume the library. And before you can depend on the library, you have to configure the library's origin repository as "backing" for your own repository.
To configure the library's repository as backing repository, navigate to the <em>Artifacts</em> section of the <em>Settings</em> tab of your repository.

This makes all libraries produced in backing repository available in this repository.
**Note that you still have to add dependency before using it (see "Adding dependency" section below).**

### Consuming from shared channel

To consume libraries published to the shared channel (<em>libs</em> in this example), you have to add following in Python Transforms project's ``transforms-python/build.gradle``:

```
apply plugin: 'com.palantir.transforms.lang.python'
apply plugin: 'com.palantir.transforms.lang.python-defaults'

transformsPython {
  sharedChannels 'libs'
}
```

or in Python Library project's ``build.gradle``, add to the `pythonLibrary` block the line `sharedChannels 'libs'` like the following:

```
apply plugin: 'com.palantir.transforms.lang.python-library'
apply plugin: 'com.palantir.transforms.lang.python-library-defaults'

pythonLibrary {
    sharedChannels 'libs'
}
```

Please note that as of conda-repository proxy verion 0.70.0 and artifacts version 0.67.10, the only available shared channel is **libs**. If other channels existed on your stack before you can still use them to publish and consume, but no new channels are allowed.

### Adding dependency

After you have setup backing repository or shared channel, you can then depend on the name of the library as usual
in the project's ``conda_recipe/meta.yaml`` file:

```
requirements:
  run:
    - python
    - my-library
```

Adding library to your project will install packages from source directory. The source directory defaults to ``src/``
and we recommend not changing this. You still need to import package before you can use it in your module. Be aware that you have to import package name and not library name (in this template, package name is ``myproject``).

*Important:* note that underscores in the repository name are rewritten to dash. For example, if your repository is named `my_library`, then the library will be published as `my-library` and should be listed as such in the ``conda_recipe/meta.yaml`` file.

### Import example

Let's say your library structure is:

```
conda_recipe/
src/
  examplepkg/
    __init__.py
    mymodule.py
  otherpkg/
    __init__.py
    utils.py
  setup.cfg
  setup.py
```

And in ``gradle.properties``, value of ``condaPackageName`` is ``mylibrary``.

When consuming library, your ``conda_recipe/meta.yaml`` should contain:

```
requirements:
  run:
    ...
    mylibrary
```

Then you can import any of the packages ``examplepkg`` or ``otherpkg``.

```
import examplepkg as E
from examplepkg import mymodule
from otherpkg.utils import some_function_in_utils
```

Note that the import may fail if the package does not include a file named __init__.py.
