import 'dart:io';
import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:path/path.dart';
import 'package:path_provider/path_provider.dart';
import './tlfs.dart' as tlfs;

/// Opens or creates a db at `~/Documents/{appname}`, loads the schema
/// package from `assets/{appname}.tlfs.rkyv and initializes the sdk.
Future<tlfs.Sdk> _loadSdk(String appname, bool persistent) async {
  final documentsDirectory = await getApplicationDocumentsDirectory();
  final dbPath = join(documentsDirectory.path, appname, 'db');
  await Directory(dbPath).create(recursive: true);
  final assetName = 'assets/$appname.tlfs.rkyv';
  final schema = await rootBundle.load(assetName);
  if (persistent) {
    return tlfs.Api.load().createPersistent(dbPath, schema.buffer.asUint8List());
  } else {
    return tlfs.Api.load().createMemory(schema.buffer.asUint8List());
  }
}

class _InheritedSdk extends InheritedWidget {
  const _InheritedSdk({
    Key? key,
    required this.data,
    required Widget child,
  }) : super(key: key, child: child);

  final SdkState data;

  @override
  bool updateShouldNotify(InheritedWidget oldWidget) => false;
}

Widget _error(String err) {
  return _SdkError(err);
}

/// Sdk widget handles loading the sdk.
class Sdk extends StatefulWidget {
  /// Creates a new Sdk widget.
  const Sdk({
    Key? key,
    required this.appname,
    required this.child,
    this.loading = const _SdkLoading(),
    this.error = _error,
    this.persistent = true,
  }) : super(key: key);

  /// The name of the app is used when creating an application folder in
  /// the documents directory and when loading the schema from the assets
  /// folder.
  final String appname;
  /// If data should be persisted.
  final bool persistent;
  /// Inner widget.
  final Widget child;
  /// Loading widget.
  final Widget loading;
  /// Error widget.
  final Widget Function(String) error;

  @override
  SdkState createState() => SdkState();

  static SdkState of(BuildContext context) {
    final _InheritedSdk? result =
        context.dependOnInheritedWidgetOfExactType<_InheritedSdk>();
    assert(result != null, 'No Sdk found in context');
    return result!.data;
  }
}

/// State for the Sdk widget.
class SdkState extends State<Sdk> {
  tlfs.Sdk? _sdk;
  String? _err;

  tlfs.Sdk get sdk => _sdk!;

  @override
  initState() {
    _loadSdk(widget.appname, widget.persistent).then((sdk) {
      setState(() {
        _sdk = sdk;
      });
    }).catchError((err) {
      setState(() {
        _err = err.toString();
      });
    });
    super.initState();
  }

  @override
  Widget build(BuildContext context) {
    if (_sdk != null) {
      return _InheritedSdk(
        data: this,
        child: widget.child,
      );
    } else if (_err != null) {
      return widget.error(_err!);
    } else {
      return widget.loading;
    }
  }

  @override
  dispose() {
    if (_sdk != null) {
      _sdk!.drop();
    }
    super.dispose();
  }
}

class _SdkLoading extends StatelessWidget {
  const _SdkLoading() : super();

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      home: Scaffold(
        body: Center(
          child: Column(
            mainAxisAlignment: MainAxisAlignment.center,
            crossAxisAlignment: CrossAxisAlignment.center,
            children: [
              SizedBox(
                child: CircularProgressIndicator(),
                width: 60,
                height: 60,
              ),
              Padding(
                padding: EdgeInsets.only(top: 16),
                child: Text('Loading sdk...'),
              )
            ],
          ),
        ),
      ),
    );
  }
}

class _SdkError extends StatelessWidget {
  const _SdkError(this.msg) : super();

  final String msg;

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      home: Scaffold(
        body: Center(
          child: Column(
            mainAxisAlignment: MainAxisAlignment.center,
            crossAxisAlignment: CrossAxisAlignment.center,
            children: [
              Text('Error loading sdk...'),
              Padding(
                padding: EdgeInsets.only(top: 16),
                child: Text(msg),
              )
            ],
          ),
        ),
      ),
    );
  }
}
